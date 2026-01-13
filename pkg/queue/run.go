package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	"github.com/mutablelogic/go-server"
	"github.com/mutablelogic/go-server/pkg/ref"
	"github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// workerPool manages registered workers for processing tasks and tickers.
// It is embedded in Manager and should not be created directly.
type workerPool struct {
	mu      sync.RWMutex
	wg      sync.WaitGroup
	tasks   map[string]Worker
	tickers map[string]Worker
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func newWorkerPool() *workerPool {
	return &workerPool{
		tasks:   make(map[string]Worker),
		tickers: make(map[string]Worker),
	}
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterQueueWorker registers a queue with its worker and creates/updates it in the database.
func (manager *Manager) RegisterQueueWorker(ctx context.Context, meta schema.QueueMeta, worker Worker) (*schema.Queue, error) {
	queue, err := manager.RegisterQueue(ctx, meta)
	if err != nil {
		return nil, err
	}
	manager.pool.mu.Lock()
	manager.pool.tasks[meta.Queue] = worker
	manager.pool.mu.Unlock()
	return queue, nil
}

// RegisterTickerWorker registers a ticker with its worker and creates/updates it in the database.
func (manager *Manager) RegisterTickerWorker(ctx context.Context, meta schema.TickerMeta, worker Worker) (*schema.Ticker, error) {
	ticker, err := manager.RegisterTicker(ctx, meta)
	if err != nil {
		return nil, err
	}
	manager.pool.mu.Lock()
	manager.pool.tickers[meta.Ticker] = worker
	manager.pool.mu.Unlock()
	return ticker, nil
}

// Run starts the cleanup ticker and all registered task and ticker workers,
// blocking until context is cancelled or an error occurs.
func (manager *Manager) Run(ctx context.Context) error {
	var result error
	var mu sync.Mutex

	// Get the logger from the context (may be nil in tests)
	var log server.Logger
	func() {
		defer func() { recover() }()
		log = ref.Log(ctx)
	}()

	// Collect registered queue names
	manager.pool.mu.RLock()
	queues := make([]string, 0, len(manager.pool.tasks))
	for q := range manager.pool.tasks {
		queues = append(queues, q)
	}
	hasTickerWorkers := len(manager.pool.tickers) > 0
	manager.pool.mu.RUnlock()

	// Run cleanup ticker in system namespace
	manager.pool.wg.Add(1)
	go func() {
		defer manager.pool.wg.Done()
		if err := manager.RunTickerLoopNs(ctx, schema.SchemaName, schema.TickerPeriod, func(ctx context.Context, ticker *schema.Ticker) error {
			if ticker.Ticker != schema.CleanupTickerName {
				return nil
			}

			// Start the span
			ctx, endspan := otel.StartSpan(manager.tracer, ctx, spanManagerName("cleanup"),
				attribute.String("ticker", ticker.String()),
			)

			// Run cleanup for all queues in this manager's namespace
			if log != nil {
				log.With("ticker", ticker.Ticker).Print(ctx, "running cleanup")
			}
			cleanupErr := manager.cleanNamespace(ctx, manager.ns)
			if cleanupErr != nil && log != nil {
				log.Print(ctx, "cleanup error ", cleanupErr)
			}

			// End the span
			endspan(cleanupErr)
			return nil
		}); err != nil {
			if !errors.Is(err, context.Canceled) {
				mu.Lock()
				result = errors.Join(result, fmt.Errorf("cleanup ticker: %w", err))
				mu.Unlock()
			}
		}
	}()

	// Run ticker loop with handler (if any tickers registered)
	if hasTickerWorkers {
		manager.pool.wg.Add(1)
		go func() {
			defer manager.pool.wg.Done()
			if err := manager.RunTickerLoop(ctx, schema.TickerPeriod, func(ctx context.Context, ticker *schema.Ticker) error {
				return manager.runTickerWorker(ctx, ticker, manager.tracer)
			}); err != nil {
				if !errors.Is(err, context.Canceled) {
					mu.Lock()
					result = errors.Join(result, fmt.Errorf("ticker loop: %w", err))
					mu.Unlock()
				}
			}
		}()
	}

	// Run task loop with handler (if any queues registered)
	if len(queues) > 0 {
		if err := manager.RunTaskLoop(ctx, func(ctx context.Context, task *schema.Task) error {
			err := manager.runTaskWorker(ctx, task, manager.tracer)
			if err != nil {
				log.With("task", task).Print(ctx, err)
			}
			return err
		}, queues...); err != nil {
			if !errors.Is(err, context.Canceled) {
				mu.Lock()
				result = errors.Join(result, fmt.Errorf("task loop: %w", err))
				mu.Unlock()
			}
		}
	}

	// Wait for all background workers to finish
	manager.pool.wg.Wait()

	return result
}

// Wait blocks until all running workers have completed.
func (pool *workerPool) Wait() {
	pool.wg.Wait()
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (manager *Manager) runTickerWorker(ctx context.Context, ticker *schema.Ticker, tracer trace.Tracer) error {
	manager.pool.mu.RLock()
	worker, ok := manager.pool.tickers[ticker.Ticker]
	manager.pool.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no worker registered for ticker %q", ticker.Ticker)
	}

	// Run the worker in the background (tickers are fire-and-forget)
	manager.pool.wg.Add(1)
	go func() {
		var result error
		defer manager.pool.wg.Done()

		// Create a timeout context based on ticker interval
		child, cancel := withTimeout(ctx, types.PtrDuration(ticker.Interval))
		defer cancel()

		// Create the span
		child2, endfunc := otel.StartSpan(tracer, child, spanManagerName("ticker."+ticker.Ticker),
			attribute.String("ticker", ticker.String()),
		)
		defer func() { endfunc(result) }()

		// Run the worker
		result = worker.Run(child2, ticker.Payload)
	}()

	return nil
}

func (manager *Manager) runTaskWorker(ctx context.Context, task *schema.Task, tracer trace.Tracer) error {
	manager.pool.mu.RLock()
	worker, ok := manager.pool.tasks[task.Queue]
	manager.pool.mu.RUnlock()
	if !ok {
		return fmt.Errorf("no worker registered for queue %q", task.Queue)
	}

	// Set deadline based on task dies_at
	child, cancel := withDeadline(ctx, types.PtrTime(task.DiesAt))
	defer cancel()

	// Create the span
	var result error
	child2, endfunc := otel.StartSpan(tracer, child, spanManagerName("task."+task.Queue),
		attribute.String("task", task.String()),
	)
	defer func() { endfunc(result) }()

	// Run the worker
	result = worker.Run(child2, task.Payload)

	// Release the task back to the queue as success/failure (use child2 to nest the span)
	var status string
	if _, releaseErr := manager.ReleaseTask(child2, task.Id, result == nil, result, &status); releaseErr != nil {
		result = errors.Join(result, releaseErr)
	}

	// If the status is not 'released', log a warning
	if status != "released" {
		result = errors.Join(result, fmt.Errorf("task status: %s", status))
	}

	// Return any errors
	return result
}

// withTimeout returns a context with timeout if duration > 0, else returns the parent context.
func withTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d > 0 {
		return context.WithTimeout(parent, d)
	}
	return parent, func() {}
}

// withDeadline returns a context with deadline if time is in the future.
// Returns the parent context unchanged if time is zero or in the past.
func withDeadline(parent context.Context, t time.Time) (context.Context, context.CancelFunc) {
	if !t.IsZero() && t.After(time.Now()) {
		return context.WithDeadline(parent, t)
	}
	return parent, func() {}
}

func spanManagerName(op string) string {
	return schema.SchemaName + ".manager." + op
}

// cleanNamespace cleans all queues in a specific namespace
func (manager *Manager) cleanNamespace(ctx context.Context, namespace string) error {
	var result error

	// List all queues in this namespace
	var queues schema.QueueList
	if err := manager.conn.With("ns", namespace).List(ctx, &queues, schema.QueueListRequest{}); err != nil {
		return err
	}

	// Clean each queue in this namespace
	for _, queue := range queues.Body {
		var resp schema.QueueCleanResponse
		if err := manager.conn.With("ns", namespace).List(ctx, &resp, schema.QueueCleanRequest{Queue: queue.Queue}); err != nil {
			// Continue cleaning other queues even if one fails
			result = errors.Join(result, err)
		}
	}

	// Return any errors
	return result
}
