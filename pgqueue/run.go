package pgqueue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"runtime"
	"slices"
	"time"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	attribute "go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// notification is the payload of a notification that a task was inserted
type notification struct {
	Schema string `json:"schema"`
	Queue  string `json:"queue"`
}

// runner is the state of the event loop while it runs
type runner struct {
	*Manager
	logger *slog.Logger

	// The results of the tasks which have run
	results chan *Result

	// The number of times in a row the queues and tickers couldn't be polled
	queueRetries  int
	tickerRetries int
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// resultsBuffer is the number of task results which can be waiting to be
	// released
	resultsBuffer = 16

	// morePeriod is how soon the queues are polled again when they may have
	// more tasks to run
	morePeriod = time.Second

	// maxRetries is the most times in a row the backoff increases after errors
	maxRetries = 5

	// maxTickers is the most tickers run each time the tickers are polled, so
	// the loop isn't held up
	maxTickers = 100
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Run runs the event loop until the context is cancelled, running the tasks
// of the queues, and the tickers, which have callbacks in this process. The
// queues and tickers of another process sharing the database are left to
// that process. When the context is cancelled, the running tasks are
// cancelled, and released as failed, so they're retried, if they have retries
// left. Only one event loop can run at a time.
func (self *Manager) Run(ctx context.Context, logger *slog.Logger) error {
	// Only one event loop can run at a time
	if !self.running.CompareAndSwap(false, true) {
		return pg.ErrConflict.With("pgqueue is already running")
	}
	defer self.running.Store(false)

	// Run the event loop
	runner := &runner{
		Manager: self,
		logger:  logger,
		results: make(chan *Result, resultsBuffer),
	}
	return runner.run(ctx)
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// run runs the event loop until the context is cancelled, and then waits for
// the running tasks to stop
func (r *runner) run(ctx context.Context) error {
	// Subscribe to notifications that tasks were inserted, until the loop
	// stops
	notifyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	notifications, err := r.Subscribe(notifyCtx, schema.DefaultNotifyChannel)
	if ctx.Err() != nil {
		// Stopped while starting
		return nil
	} else if err != nil {
		return err
	}

	// The loop is ready once it's listening for new tasks
	r.ready.Store(true)
	defer r.ready.Store(false)

	// Poll the queues and tickers straight away
	queueTimer := time.NewTimer(0)
	defer queueTimer.Stop()
	tickerTimer := time.NewTimer(0)
	defer tickerTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			r.shutdown(ctx)
			return nil
		case notification, ok := <-notifications:
			if !ok {
				// The subscription is closed when the listener fails, or the
				// pool is reset, such as when partitions are created, so the
				// loop isn't ready until it subscribes again, when the queues
				// are next polled, soon
				r.ready.Store(false)
				notifications = nil
				if ctx.Err() == nil {
					r.logger.DebugContext(ctx, "Stopped listening for new tasks", "channel", schema.DefaultNotifyChannel)
					queueTimer.Reset(morePeriod)
				}
			} else if r.notified(notification) {
				queueTimer.Reset(0)
			}
		case result := <-r.results:
			// A task finished, so another may be run
			if r.release(ctx, result) {
				queueTimer.Reset(0)
			}
		case <-queueTimer.C:
			// Subscribe again, before the queues are polled, so no new tasks
			// are missed
			if notifications == nil {
				if notifications, err = r.Subscribe(notifyCtx, schema.DefaultNotifyChannel); err != nil {
					notifications = nil
					r.logger.WarnContext(ctx, "Failed to listen for new tasks", "channel", schema.DefaultNotifyChannel, "error", err.Error())
				} else {
					r.ready.Store(true)
				}
			}
			queueTimer.Reset(r.runQueues(ctx))
		case <-tickerTimer.C:
			tickerTimer.Reset(r.runTickers(ctx))
		}
	}
}

// notified returns true if the notification is that a task was inserted into
// one of the queues with callbacks in this process
func (r *runner) notified(n pg.Notification) bool {
	var payload notification
	if err := json.Unmarshal(n.Payload, &payload); err != nil {
		return false
	} else if payload.Schema != r.schema {
		return false
	}
	return slices.Contains(r.queues.Names(), payload.Queue)
}

// runQueues runs the tasks of the queues which have callbacks in this
// process, and returns how long until the queues are polled again: soon when
// there may be more tasks, and backing off after errors
func (r *runner) runQueues(ctx context.Context) time.Duration {
	// A process only runs the tasks of its own queues: another process
	// sharing the database runs the others
	queues := r.queues.Names()
	if len(queues) == 0 {
		return schema.DefaultQueuePeriod
	}

	// Run the tasks, and back off after errors
	more, err := r.runTasks(ctx, queues)
	if err != nil && ctx.Err() == nil {
		r.logger.ErrorContext(ctx, "Failed to run tasks", "queues", queues, "error", err.Error())
	}
	var period time.Duration
	if r.queueRetries, period = backoffPeriod(r.queueRetries, schema.DefaultQueuePeriod, err != nil); err == nil && more {
		period = morePeriod
	}
	return period
}

// runTasks retains tasks from the queues with callbacks in this process, and
// runs them, at most GOMAXPROCS each time, so the loop isn't held up. Returns
// true if there may be more tasks to run.
func (r *runner) runTasks(ctx context.Context, queues []string) (more bool, err error) {
	ctx, endSpan := otel.StartSpan(r.tracer, ctx, "pgqueue.runTasks",
		attribute.StringSlice("queues", queues),
	)
	defer func() { endSpan(err) }()

	for range runtime.GOMAXPROCS(0) {
		if ran, err := r.runTask(ctx); err != nil || !ran {
			return false, err
		}
	}

	// Return success: there may be more tasks
	return true, nil
}

// runTask retains a task from the queues with callbacks in this process, and
// runs it, and returns true if there was a task to run. A queue's callback
// isn't unregistered while a task is retained and run, so a task retained
// from a queue is run with its callback.
func (r *runner) runTask(ctx context.Context) (bool, error) {
	r.dispatch.RLock()
	defer r.dispatch.RUnlock()

	queues := r.queues.Names()
	if len(queues) == 0 {
		return false, nil
	}
	task, err := r.NextTask(ctx, r.worker, queues...)
	if err != nil || task == nil {
		return false, err
	}

	// The result is released when the task finishes
	r.queues.RunQueueTask(ctx, task, r.results)
	return true, nil
}

// runTickers runs the due tickers of the tickers with callbacks in this
// process, and returns how long until the tickers are polled again, backing
// off after errors
func (r *runner) runTickers(ctx context.Context) time.Duration {
	err := r.runDueTickers(ctx)
	if err != nil && ctx.Err() == nil {
		r.logger.ErrorContext(ctx, "Failed to run tickers", "error", err.Error())
	}
	var period time.Duration
	r.tickerRetries, period = backoffPeriod(r.tickerRetries, schema.DefaultTickerPeriod, err != nil)
	return period
}

// runDueTickers runs every due ticker, up to maxTickers, so tickers which are
// due at the same time don't wait for the next poll
func (r *runner) runDueTickers(ctx context.Context) (err error) {
	ctx, endSpan := otel.StartSpan(r.tracer, ctx, "pgqueue.runTickers")
	defer func() { endSpan(err) }()

	for range maxTickers {
		ticker, err := r.NextTicker(ctx)
		if err != nil {
			return err
		} else if ticker == nil {
			return nil
		}
		if err := r.tickers.RunTickerTask(ctx, ticker, r.results); errors.Is(err, httpresponse.ErrNotFound) {
			// The ticker was unregistered after it was found to be due
			continue
		} else if err != nil {
			return err
		}
	}

	// Return success
	return nil
}

// release the result of a task, as done or failed, and returns
// true, or logs the result of a ticker, and returns false. A task which was
// cancelled because the event loop is stopping is released as failed, so it's
// retried, if it has retries left.
func (r *runner) release(ctx context.Context, result *Result) bool {
	if result == nil {
		return false
	} else if result.Task == nil {
		r.logTicker(ctx, result)
		return false
	}

	// The result is released even when the loop is stopping, in the task's
	// trace
	releaseCtx := context.WithoutCancel(ctx)
	if result.Trace.IsValid() {
		releaseCtx = trace.ContextWithSpanContext(releaseCtx, result.Trace)
	}
	task := result.Task

	// A failed task's result is its error
	success := result.Error == nil
	data := result.Result
	if !success {
		var err error
		if data, err = json.Marshal(result.Error.Error()); err != nil {
			r.logger.ErrorContext(releaseCtx, "Failed to encode task error", "queue", result.Queue, "task", task.Id, "error", err.Error())
			return true
		}
	}

	// Release the task. A task which ran past its TTL may have been retained
	// by another worker, so its result is ignored.
	var status string
	if _, err := r.ReleaseTask(releaseCtx, task.Id, success, data, &status); errors.Is(err, pg.ErrNotFound) {
		r.logger.WarnContext(releaseCtx, "Late task result ignored", "queue", result.Queue, "task", task.Id)
	} else if err != nil {
		r.logger.ErrorContext(releaseCtx, "Failed to release task", "queue", result.Queue, "task", task.Id, "error", err.Error())
	} else if result.Error != nil {
		r.logger.ErrorContext(releaseCtx, "Task failed", "queue", result.Queue, "task", task.Id, "status", status, "error", result.Error.Error())
	} else {
		r.logger.DebugContext(releaseCtx, "Task done", "queue", result.Queue, "task", task.Id, "status", status)
	}
	return true
}

// logTicker logs the result of a ticker
func (r *runner) logTicker(ctx context.Context, result *Result) {
	stopping := ctx.Err() != nil
	if result.Trace.IsValid() {
		ctx = trace.ContextWithSpanContext(context.WithoutCancel(ctx), result.Trace)
	}
	if result.Error != nil && stopping && errors.Is(result.Error, context.Canceled) {
		// Cancelled because the loop is stopping
		r.logger.DebugContext(ctx, "Ticker cancelled", "ticker", result.Ticker)
	} else if result.Error != nil {
		r.logger.ErrorContext(ctx, "Ticker failed", "ticker", result.Ticker, "error", result.Error.Error())
	} else if len(result.Result) > 0 && !bytes.Equal(result.Result, []byte("null")) {
		r.logger.DebugContext(ctx, "Ticker done", "ticker", result.Ticker, "result", string(result.Result))
	}
}

// shutdown waits for the running tasks and tickers, which are cancelled, to
// stop, and releases the results of the tasks
func (r *runner) shutdown(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		r.queues.Close()
		r.tickers.Close()
		close(done)
	}()
	for {
		select {
		case result := <-r.results:
			r.release(ctx, result)
		case <-done:
			// Release the results sent before the tasks stopped
			for {
				select {
				case result := <-r.results:
					r.release(ctx, result)
				default:
					return
				}
			}
		}
	}
}

// backoffPeriod returns the number of errors in a row, and the period until
// the next attempt, which increases with each error, up to maxRetries
func backoffPeriod(retries int, period time.Duration, err bool) (int, time.Duration) {
	if err {
		retries = min(retries+1, maxRetries)
		return retries, time.Duration(retries*retries) * period
	}
	return 0, period
}
