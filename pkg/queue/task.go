package queue

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - TASK

// CreateTask creates a new task, and returns it.
func (manager *Manager) CreateTask(ctx context.Context, queue string, meta schema.TaskMeta) (*schema.Task, error) {
	var taskId schema.TaskId
	var task schema.TaskWithStatus

	// Validate payload is not nil
	if meta.Payload == nil {
		return nil, errors.New("missing payload")
	}

	// Insert the task, and return it
	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		if err := conn.With("id", queue).Insert(ctx, &taskId, meta); err != nil {
			return err
		}
		return conn.Get(ctx, &task, taskId)
	}); err != nil {
		return nil, err
	}

	// Return the task
	return &task.Task, nil
}

// ListTasks returns all tasks in a namespace as a list, with optional filtering
func (manager *Manager) ListTasks(ctx context.Context, req schema.TaskListRequest) (*schema.TaskList, error) {
	var list schema.TaskList
	if err := manager.conn.List(ctx, &list, req); err != nil {
		return nil, err
	}
	return &list, nil
}

// NextTask retains a task from any of the specified queues, and returns it.
// If queues is empty, tasks from any queue are considered.
// Returns nil if there is no task to retain.
func (manager *Manager) NextTask(ctx context.Context, worker string, queues ...string) (*schema.Task, error) {
	var taskId schema.TaskId
	var task schema.TaskWithStatus

	// Insert the task, and return it
	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		if err := conn.Get(ctx, &taskId, schema.TaskRetain{
			Queues: queues,
			Worker: worker,
		}); err != nil {
			return err
		}

		// No task to retain
		if taskId == 0 {
			return nil
		}

		// Get the task
		return conn.Get(ctx, &task, taskId)
	}); err != nil {
		return nil, err
	}

	// Return task
	if taskId == 0 {
		return nil, nil
	} else {
		return &task.Task, nil
	}
}

// ReleaseTask releases a task from a queue, and returns it. Can optionally set the status
func (manager *Manager) ReleaseTask(ctx context.Context, task uint64, success bool, result any, status *string) (*schema.Task, error) {
	var taskId schema.TaskId
	var taskObj schema.TaskWithStatus

	// Release the task, and return it
	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		if err := conn.Get(ctx, &taskId, schema.TaskRelease{Id: task, Fail: !success, Result: result}); err != nil {
			return err
		}

		// No task found
		if taskId == 0 {
			return pg.ErrNotFound
		}

		// Get the task
		return conn.Get(ctx, &taskObj, taskId)
	}); err != nil {
		return nil, err
	}

	// Optionally set the status
	if status != nil {
		*status = taskObj.Status
	}

	// Return task
	return &taskObj.Task, nil
}

// RunTaskLoop runs a task loop with N concurrent workers.
// Each worker calls the handler function when a task is received.
// The handler should return nil on success, or an error to indicate failure.
// Blocks until context is cancelled. If queues is empty, tasks from any queue are considered.
// The worker name (from WithWorkerName) will have @N appended for each concurrent worker (e.g., "hostname@1").
// The number of workers is set via WithWorkers option.
func (manager *Manager) RunTaskLoop(ctx context.Context, handler TaskHandler, queues ...string) error {
	workers := manager.workers
	if workers < 1 {
		workers = 1
	}

	// Semaphore to limit concurrent workers
	sem := make(chan int, workers)
	for i := 1; i <= workers; i++ {
		sem <- i
	}

	// Run the task loop - blocks until ctx cancelled or error
	return manager.runTaskLoop(ctx, sem, handler, queues)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS - TASK

// runTaskLoop is the internal loop that fetches tasks and dispatches to workers.
func (manager *Manager) runTaskLoop(ctx context.Context, sem chan int, handler TaskHandler, queues []string) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	// Create listener for task notifications
	listener := manager.conn.Listener()
	if listener == nil {
		return pg.ErrBadParameter.With("listener is nil")
	}
	defer listener.Close(context.Background())

	// Subscribe to queue insert notifications for this namespace
	topic := manager.ns + schema.TopicQueueInsert
	if err := listener.Listen(ctx, topic); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	// Notification and error channels
	notifyCh := make(chan *pg.Notification)
	listenerErr := make(chan error, 1)

	// Start notification listener
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := listenForNotifications(ctx, listener, notifyCh); err != nil {
			listenerErr <- err
		}
	}()

	// Polling timer
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-listenerErr:
			return err
		case n := <-notifyCh:
			if err := manager.drainTasks(ctx, n, queues, sem, handler, &wg, timer); err != nil {
				return err
			}
		case <-timer.C:
			if err := manager.pollForTasks(ctx, queues, sem, handler, &wg, timer); err != nil {
				return err
			}
			timer.Reset(schema.TaskPeriod)
		}
	}
}

// drainTasks processes a notification and drains all available tasks.
func (manager *Manager) drainTasks(ctx context.Context, n *pg.Notification, queues []string, sem chan int, handler TaskHandler, wg *sync.WaitGroup, timer *time.Timer) error {
	// Check if notification is for our queues (empty means accept any)
	if len(queues) > 0 && !slices.Contains(queues, string(n.Payload)) {
		return nil
	}

	// Create a span for the task retention operation to nest NextTask database calls
	ctx, endspan := otel.StartSpan(manager.tracer, ctx, spanManagerName("task.retain"))
	defer endspan(nil)

	for {
		// Wait for available worker slot
		select {
		case <-ctx.Done():
			return nil
		case workerNum := <-sem:
			workerName := fmt.Sprintf("%s@%d", manager.name, workerNum)
			task, err := manager.NextTask(ctx, workerName, queues...)
			if err != nil {
				sem <- workerNum // Return slot
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil
				}
				return err
			}
			if task == nil {
				sem <- workerNum // Return slot
				return nil
			}
			// Dispatch to worker goroutine
			wg.Add(1)
			go func(num int, t *schema.Task) {
				defer wg.Done()
				defer func() { sem <- num }()
				handler(ctx, t)
			}(workerNum, task)
			timer.Reset(100 * time.Millisecond)
		}
	}
}

// pollForTasks periodically checks for available tasks (polling fallback)
func (manager *Manager) pollForTasks(ctx context.Context, queues []string, sem chan int, handler TaskHandler, wg *sync.WaitGroup, timer *time.Timer) error {
	// Create a span for the task retention operation to nest NextTask database calls
	ctx, endspan := otel.StartSpan(manager.tracer, ctx, spanManagerName("task.retain"))
	defer endspan(nil)

	for {
		// Wait for available worker slot
		select {
		case <-ctx.Done():
			return nil
		case workerNum := <-sem:
			workerName := fmt.Sprintf("%s@%d", manager.name, workerNum)
			task, err := manager.NextTask(ctx, workerName, queues...)
			if err != nil {
				sem <- workerNum // Return slot
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil
				}
				return err
			}
			if task == nil {
				sem <- workerNum // Return slot
				return nil
			}
			// Dispatch to worker goroutine
			wg.Add(1)
			go func(num int, t *schema.Task) {
				defer wg.Done()
				defer func() { sem <- num }()
				handler(ctx, t)
			}(workerNum, task)
			timer.Reset(100 * time.Millisecond)
		}
	}
}

// listenForNotifications forwards PostgreSQL notifications to a channel.
// Returns nil on context cancellation, or the error on failure.
func listenForNotifications(ctx context.Context, listener pg.Listener, ch chan<- *pg.Notification) error {
	for {
		n, err := listener.WaitForNotification(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		select {
		case ch <- n:
		case <-ctx.Done():
			return nil
		}
	}
}
