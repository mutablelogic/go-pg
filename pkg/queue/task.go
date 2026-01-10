package queue

import (
	"context"
	"errors"
	"sync"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - TASK

// CreateTask creates a new task, and returns it.
func (manager *Manager) CreateTask(ctx context.Context, queue string, meta schema.TaskMeta) (*schema.Task, error) {
	var taskId schema.TaskId
	var task schema.TaskWithStatus

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

// RunTaskLoop runs a loop to process tasks, until the context is cancelled
// or an error occurs. It uses both polling and LISTEN/NOTIFY to pick up tasks
// immediately when they're created.
// If queues is empty, tasks from any queue are considered.
func (manager *Manager) RunTaskLoop(ctx context.Context, ch chan<- *schema.Task, worker string, queues ...string) error {
	delta := schema.TaskPeriod
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	// Create listener for task notifications
	listener := manager.conn.Listener()
	if listener == nil {
		return pg.ErrBadParameter.With("listener is nil")
	}
	defer listener.Close(context.Background())

	// Subscribe to queue insert notifications for this namespace
	topic := manager.ns + "_queue_insert"
	if err := listener.Listen(ctx, topic); err != nil {
		// If context is canceled, return nil (not an error)
		if errors.Is(err, context.Canceled) {
			return nil
		} else {
			return err
		}
	}

	// Create channels for notifications and errors
	notifyCh := make(chan *pg.Notification, 10)
	errCh := make(chan error, 1)

	// Start goroutine to listen for notifications
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()
	go func() {
		defer wg.Done()
		listenForTaskNotifications(ctx, listener, notifyCh, errCh)
	}()

	// Do an initial poll immediately to pick up any existing tasks
	if err := manager.pollForTasks(ctx, queues, worker, ch, &delta); err != nil {
		return err
	}
	timer.Reset(delta)

	// Loop until context is cancelled
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			return err
		case notification := <-notifyCh:
			if err := manager.handleTaskNotification(ctx, notification, queues, worker, ch, timer, &delta); err != nil {
				return err
			}
		case <-timer.C:
			if err := manager.pollForTasks(ctx, queues, worker, ch, &delta); err != nil {
				return err
			}
			timer.Reset(delta)
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// listenForTaskNotifications listens for PostgreSQL notifications about new tasks
// and forwards them to the notification channel. Errors (except context cancellation)
// are sent to the error channel.
func listenForTaskNotifications(ctx context.Context, listener pg.Listener, notifyCh chan<- *pg.Notification, errCh chan<- error) {
	for {
		notification, err := listener.WaitForNotification(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				errCh <- err
			}
			return
		}
		notifyCh <- notification
	}
}

// handleTaskNotification processes a notification by checking if it's for our queues
// and attempting to get and send the task.
func (manager *Manager) handleTaskNotification(ctx context.Context, notification *pg.Notification, queues []string, worker string, ch chan<- *schema.Task, timer *time.Timer, delta *time.Duration) error {
	// Check if notification is for our queues (empty queues means accept any)
	if len(queues) > 0 {
		match := false
		for _, q := range queues {
			if string(notification.Payload) == q {
				match = true
				break
			}
		}
		if !match {
			return nil
		}
	}

	// Try to get all available tasks
	for {
		task, err := manager.NextTask(ctx, worker, queues...)
		if err != nil {
			// Context errors are not errors - just stop handling
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}

		if task != nil {
			ch <- task
			*delta = 100 * time.Millisecond
			timer.Reset(*delta)
			// Continue to get next task immediately
		} else {
			return nil
		}
	}
}

// pollForTasks periodically checks for available tasks (polling fallback)
func (manager *Manager) pollForTasks(ctx context.Context, queues []string, worker string, ch chan<- *schema.Task, delta *time.Duration) error {
	// Try to get all available tasks in a loop
	for {
		task, err := manager.NextTask(ctx, worker, queues...)
		if err != nil {
			// Context errors are not errors - just stop polling
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}

		if task != nil {
			ch <- task
			*delta = 100 * time.Millisecond
			// Continue to get next task immediately
		} else {
			// No more tasks available, slow down polling
			*delta = schema.TaskPeriod
			return nil
		}
	}
}
