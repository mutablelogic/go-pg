package manager

import (
	"context"
	"encoding/json"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"time"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	trace "go.opentelemetry.io/otel/trace"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) Run(ctx context.Context, log *slog.Logger) error {
	// Timer for next ticker
	timer_retries, timer_period := 0, schema.DefaultTickerPeriod
	timer := time.NewTimer(timer_period)
	defer timer.Stop()

	// Timer for delayed/failed queue tasks
	queue_retries, queue_period := 0, schema.DefaultQueuePeriod
	queueTimer := time.NewTimer(time.Second * 5)
	defer queueTimer.Stop()

	// Notification of new tasks in queues
	notify_ctx, notify_cancel := context.WithCancel(ctx)
	defer notify_cancel()
	notifications, err := manager.Subscribe(notify_ctx, schema.DefaultNotifyChannel)
	if err != nil {
		return err
	}

	// Shared completion channel for ticker callbacks
	results := make(chan *Result, 16)
	defer close(results)
	releaseCtx := context.WithoutCancel(ctx)

	// Shutdown handling
	ctxDone := ctx.Done()
	timerC := timer.C
	queueTimerC := queueTimer.C
	shutdownDone := make(chan struct{})
	// Close is done later

	// Tick handler
	tick := func(trigger string) (err error) {
		// Otel span
		child, endSpan := otel.StartSpan(manager.tracer, ctx, strings.Join([]string{"tick", trigger}, "."))
		defer func() { endSpan(err) }()

		// Get matured ticker
		ticker, err := manager.NextTicker(child)
		if err != nil {
			log.ErrorContext(ctx, "NextTicker failed", "trigger", trigger, "error", err.Error())
			return err
		} else if ticker == nil {
			return nil
		} else if err = manager.tickers.RunTickerTask(child, ticker, results); err != nil {
			log.ErrorContext(ctx, "RunTickerTask failed", "trigger", trigger, "ticker", ticker, "error", err.Error())
			return err
		}
		return nil
	}

	// Queue handler
	queue := func(name string) (more bool, err error) {
		// Otel span
		spanName := "queue.any"
		if name != "" {
			spanName = strings.Join([]string{"queue", name}, ".")
		}
		child, endSpan := otel.StartSpan(manager.tracer, ctx, spanName)
		defer func() { endSpan(err) }()

		// Process as many tasks as we have capacity for, until there are
		// no more tasks or an error occurs.
		processed := false
		for i := 0; i < runtime.GOMAXPROCS(0); i++ {
			// Get next task for the queue
			var task *schema.Task
			if name == "" {
				task, err = manager.NextTask(child, manager.worker)
			} else {
				task, err = manager.NextTask(child, manager.worker, name)
			}
			if err != nil {
				return false, err
			} else if task == nil {
				// No more tasks can be retained right now. If we processed at
				// least one task in this pass, ask the scheduler to check again
				// soon to keep draining the queue.
				return processed, nil
			}

			// Run the task callback - results are logged in the callback,
			// so we don't need to do anything with them here.
			manager.queues.RunQueueTask(child, task, results)
			processed = true
		}

		// Return success
		return true, nil
	}

	// The run loop
	var wg sync.WaitGroup
	for {
		select {
		case <-ctxDone:
			// Stop scheduling new work, but keep the loop alive to drain in-flight results.
			ctxDone = nil
			timerC = nil
			queueTimerC = nil
			notify_cancel()

			// Wait for remaining tasks in the background to finish before returning from Run.
			wg.Go(func() {
				manager.tickers.Close()
				manager.queues.Close()
				close(shutdownDone)
			})
		case <-shutdownDone:
			wg.Wait()
			return nil
		case notification, ok := <-notifications:
			if !ok {
				notifications = nil
				continue
			}
			if payload := decodeNotification(notification); payload != nil {
				if payload.Schema != "" && payload.Schema != manager.schema {
					continue
				}
				if more, err := queue(payload.Queue); err != nil {
					log.ErrorContext(ctx, "RunQueueTask failed", "queue", payload.Queue, "error", err.Error())
				} else if more {
					queueTimer.Reset(time.Second) // immediately check for more tasks in the queue
				} else {
					queueTimer.Reset(schema.DefaultQueuePeriod)
				}
			}
		case result := <-results:
			switch {
			case result != nil && result.Task != nil:
				resultCtx := releaseCtx
				if result.Trace.IsValid() {
					resultCtx = trace.ContextWithSpanContext(resultCtx, result.Trace)
				}
				// Completed queue task
				success := result.Error == nil
				releaseResult := result.Result
				if result.Error != nil {
					data, err := json.Marshal(result.Error.Error())
					if err != nil {
						log.ErrorContext(resultCtx, "Marshal queue task error failed", "queue", result.Queue, "task", result.Task.Id, "error", err.Error())
						continue
					}
					releaseResult = data
				}
				status := ""
				if _, err := manager.ReleaseTask(resultCtx, result.Task.Id, success, releaseResult, &status); err != nil {
					log.ErrorContext(resultCtx, "ReleaseTask failed", "queue", result.Queue, "task", result.Task.Id, "error", err.Error())
					continue
				}
				if result.Error != nil {
					log.ErrorContext(resultCtx, "RunQueueTask result failed", "queue", result.Queue, "task", result.Task.Id, "status", status, "error", result.Error.Error())
				} else {
					log.InfoContext(resultCtx, "RunQueueTask result", "queue", result.Queue, "task", result.Task.Id, "status", status, "result", result)
				}
				continue
			case result != nil && result.Ticker != "":
				resultCtx := ctx
				if result.Trace.IsValid() {
					resultCtx = trace.ContextWithSpanContext(resultCtx, result.Trace)
				}
				// Completed ticker task
				if result.Error != nil {
					log.ErrorContext(resultCtx, "RunTickerTask result failed", "ticker", result.Ticker, "error", result.Error.Error())
				} else {
					log.InfoContext(resultCtx, "RunTickerTask result", "ticker", result.Ticker, "result", result)
				}
			}
		case <-timerC:
			timer_retries, timer_period = backoffPeriod(timer_retries, schema.DefaultTickerPeriod, tick("timer") != nil)
			resetTimer(timer, timer_period)
		case <-queueTimerC:
			if more, err := queue(""); err != nil {
				queue_retries, queue_period = backoffPeriod(queue_retries, schema.DefaultQueuePeriod, true)
			} else {
				queue_retries, queue_period = backoffPeriod(queue_retries, schema.DefaultQueuePeriod, false)
				if more {
					queue_period = time.Second // immediately check for more tasks in the queue
				}
			}
			resetTimer(queueTimer, queue_period)
		}
	}
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func backoffPeriod(retries int, dur time.Duration, err bool) (int, time.Duration) {
	const maxRetries = 5
	if err {
		nextRetries := min(retries+1, maxRetries)
		return nextRetries, time.Duration(nextRetries*nextRetries) * dur
	}
	return 0, dur
}

func resetTimer(timer *time.Timer, dur time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(dur)
}

type queueNotification struct {
	Schema string `json:"schema"`
	Queue  string `json:"queue"`
}

func decodeNotification(notification pg.Notification) *queueNotification {
	var payload queueNotification
	if err := json.Unmarshal(notification.Payload, &payload); err != nil {
		return nil
	}
	return types.Ptr(payload)
}
