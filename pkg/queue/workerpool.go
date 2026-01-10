package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// TaskHandler processes a task. Return nil on success, or an error to fail the task.
type TaskHandler func(context.Context, *schema.Task) error

// TickerHandler processes a matured ticker. Return nil on success, or an error to stop.
type TickerHandler func(context.Context, *schema.Ticker) error

// WorkerPool manages workers for processing tasks and tickers.
type WorkerPool struct {
	manager *Manager
	opts    opts
	tasks   map[string]TaskHandler
	tickers map[string]TickerHandler
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewWorkerPool creates a new worker pool for the given manager.
func NewWorkerPool(manager *Manager, opt ...Opt) (*WorkerPool, error) {
	o, err := applyOpts(opt)
	if err != nil {
		return nil, err
	}

	return &WorkerPool{
		manager: manager,
		opts:    o,
		tasks:   make(map[string]TaskHandler),
		tickers: make(map[string]TickerHandler),
	}, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterQueue registers a queue with its handler and creates/updates it in the database.
func (wp *WorkerPool) RegisterQueue(ctx context.Context, meta schema.QueueMeta, handler TaskHandler) (*schema.Queue, error) {
	queue, err := wp.manager.RegisterQueue(ctx, meta)
	if err != nil {
		return nil, err
	}
	wp.tasks[meta.Queue] = handler
	return queue, nil
}

// RegisterTicker registers a handler for the named ticker and creates/updates it in the database.
func (wp *WorkerPool) RegisterTicker(ctx context.Context, meta schema.TickerMeta, handler TickerHandler) (*schema.Ticker, error) {
	ticker, err := wp.manager.RegisterTicker(ctx, meta)
	if err != nil {
		return nil, err
	}
	wp.tickers[meta.Ticker] = handler
	return ticker, nil
}

// Run starts all workers and blocks until context is cancelled or an error occurs.
func (wp *WorkerPool) Run(ctx context.Context) error {
	var loopWg, workerWg sync.WaitGroup
	errCh := make(chan error, 1)

	// Create work channel and spawn workers
	workCh := make(chan func(), wp.opts.workers)
	for i := 0; i < wp.opts.workers; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for fn := range workCh {
				fn()
			}
		}()
	}

	// Get list of registered queue names
	queues := make([]string, 0, len(wp.tasks))
	for q := range wp.tasks {
		queues = append(queues, q)
	}

	// Start single task loop for all queues
	if len(wp.tasks) > 0 {
		loopWg.Add(1)
		go func() {
			defer loopWg.Done()
			if err := wp.runTaskLoop(ctx, workCh, queues); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}()
	}

	// Start ticker loop
	if len(wp.tickers) > 0 {
		loopWg.Add(1)
		go func() {
			defer loopWg.Done()
			if err := wp.runTickerLoop(ctx, workCh); err != nil {
				select {
				case errCh <- err:
				default:
				}
			}
		}()
	}

	// Wait for loops to finish, close channel, wait for workers
	done := make(chan struct{})
	go func() {
		loopWg.Wait()
		close(workCh)
		workerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case err := <-errCh:
		return err
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (wp *WorkerPool) runTaskLoop(ctx context.Context, workCh chan<- func(), queues []string) error {
	// Use buffered channel to allow RunTaskLoop to fetch multiple tasks without blocking
	ch := make(chan *schema.Task, wp.opts.workers)
	var loopErr error
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(ch)
		loopErr = wp.manager.RunTaskLoop(ctx, ch, wp.opts.name, queues...)
	}()

	for task := range ch {
		// Find handler for this task's queue
		handler, ok := wp.tasks[task.Queue]
		if !ok {
			// No handler - release as failed
			var status string
			wp.manager.ReleaseTask(ctx, task.Id, false, fmt.Errorf("no handler for queue %q", task.Queue), &status)
			continue
		}

		// Calculate deadline from DiesAt
		var deadline time.Duration
		if task.DiesAt != nil {
			deadline = time.Until(*task.DiesAt)
		}

		// Fire-and-forget dispatch to worker
		t := task
		h := handler
		workCh <- func() {
			err := runWork(ctx, deadline, func(c context.Context) error {
				return h(c, t)
			})
			// Release task in worker
			var status string
			wp.manager.ReleaseTask(ctx, t.Id, err == nil, err, &status)
		}
	}

	wg.Wait()
	return loopErr
}

func (wp *WorkerPool) runTickerLoop(ctx context.Context, workCh chan<- func()) error {
	ch := make(chan *schema.Ticker)
	var loopErr error
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(ch)
		loopErr = wp.manager.RunTickerLoop(ctx, ch, wp.opts.period)
	}()

	for ticker := range ch {
		handler, ok := wp.tickers[ticker.Ticker]
		if !ok {
			continue
		}

		// Calculate deadline from Interval
		var deadline time.Duration
		if ticker.Interval != nil {
			deadline = time.Duration(*ticker.Interval)
		}

		// Dispatch to worker
		done := make(chan error, 1)
		t := ticker
		h := handler
		workCh <- func() {
			done <- runWork(ctx, deadline, func(c context.Context) error {
				return h(c, t)
			})
		}

		// Wait for result - errors are logged only, continue processing
		<-done
		// TODO: log error if err != nil
	}

	wg.Wait()
	return loopErr
}

// runWork executes work with deadline and panic recovery.
func runWork(parent context.Context, deadline time.Duration, fn func(context.Context) error) (errs error) {
	ctx, cancel := contextWithDeadline(parent, deadline)
	defer cancel()

	// Catch panics
	defer func() {
		if r := recover(); r != nil {
			errs = errors.Join(errs, fmt.Errorf("panic: %v", r))
		}
	}()

	// Run the work function
	if err := fn(ctx); err != nil {
		errs = errors.Join(errs, err)
	}

	// Include context error if not already present
	if ctx.Err() != nil && !errors.Is(errs, ctx.Err()) {
		errs = errors.Join(errs, ctx.Err())
	}

	return errs
}

func contextWithDeadline(ctx context.Context, deadline time.Duration) (context.Context, context.CancelFunc) {
	if deadline > 0 {
		return context.WithTimeout(ctx, deadline)
	}
	return ctx, func() {}
}
