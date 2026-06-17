package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
	trace "go.opentelemetry.io/otel/trace"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type exec struct {
	sync.RWMutex
	t      map[string]schema.TaskFunc
	wg     sync.WaitGroup
	tracer trace.Tracer
}

type Result struct {
	Queue  string            `json:"queue,omitempty"`
	Task   *schema.Task      `json:"task,omitempty"`
	Ticker string            `json:"ticker,omitempty"`
	Result json.RawMessage   `json:"result,omitempty"`
	Error  error             `json:"error,omitempty"`
	Trace  trace.SpanContext `json:"-"`
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func NewExec(tracer trace.Tracer) *exec {
	self := new(exec)
	self.t = make(map[string]schema.TaskFunc)
	self.tracer = tracer
	return self
}

// Wait for all tasks to complete
func (exec *exec) Close() {
	exec.wg.Wait()
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterTask stores a named task callback. Names are normalized to lowercase
// identifiers and must be unique.
func (exec *exec) RegisterTask(name string, fn schema.TaskFunc) error {
	exec.Lock()
	defer exec.Unlock()

	// Validate the input parameters
	name, err := schema.TickerName(name).Validate()
	if err != nil {
		return err
	} else if fn == nil {
		return httpresponse.ErrBadRequest.With("missing task callback")
	}

	// Check if the task name already exists
	if _, exists := exec.t[name]; exists {
		return httpresponse.ErrConflict.Withf("task %q already registered", name)
	}
	exec.t[name] = fn

	// Return success
	return nil
}

// RemoveTask removes a named task callback.
func (exec *exec) RemoveTask(name string) error {
	exec.Lock()
	defer exec.Unlock()

	// Validate the name and normalize it to lowercase identifier
	name, err := schema.TickerName(name).Validate()
	if err != nil {
		return err
	}

	// Delete the task if it exists, otherwise return not found error
	if _, exists := exec.t[name]; !exists {
		return httpresponse.ErrNotFound.Withf("task %q not found", name)
	} else {
		delete(exec.t, name)
	}

	// Return success
	return nil
}

// RunTickerTask executes a named task callback with the given payload.
func (exec *exec) RunTickerTask(ctx context.Context, ticker *schema.Ticker, result chan<- *Result) error {
	exec.RLock()
	defer exec.RUnlock()

	// TODO: Add the ticker into the context

	// Get the task function for the ticker's name
	fn, exists := exec.t[ticker.Ticker]
	if !exists {
		return httpresponse.ErrNotFound.Withf("task callback %q not found", ticker.Ticker)
	}

	// Create a deadline for the task execution based on the ticker's period
	// and the current time. This ensures that the task will not run indefinitely
	// and will be cancelled if it exceeds the ticker's period.
	deadline := time.Now().Add(time.Minute)
	if interval := types.Value(ticker.Interval); interval > 0 {
		deadline = time.Now().Add(interval)
	}

	// Run the task function with the provided payload and deadline
	child, cancel := context.WithDeadline(ctx, deadline)
	exec.wg.Go(func() {
		defer cancel()

		// Otel span
		spanCtx, endSpan := otel.StartSpan(exec.tracer, child, strings.Join([]string{"ticker", ticker.Ticker}, "."),
			attribute.String("ticker", types.Stringify(ticker)),
		)

		resp := run(spanCtx, fn, ticker.Payload)
		if resp == nil {
			resp = new(Result)
		}
		resp.Trace = trace.SpanContextFromContext(spanCtx)
		endSpan(resp.Error)
		resp.Ticker = ticker.Ticker
		result <- resp
	})

	// Return success
	return nil
}

// RunQueueTask executes a named queue callback with the given payload.
func (exec *exec) RunQueueTask(ctx context.Context, task *schema.Task, result chan<- *Result) {
	exec.RLock()
	defer exec.RUnlock()

	if task.DiesAt == nil {
		result <- &Result{Queue: task.Queue, Task: task, Error: httpresponse.ErrBadRequest.With("missing task deadline")}
		return
	}

	// TODO: Add the task into the context

	// Get the task function for the ticker's name
	fn, exists := exec.t[task.Queue]
	if !exists {
		result <- &Result{Queue: task.Queue, Task: task, Error: httpresponse.ErrNotFound.Withf("task callback %q not found", task.Queue)}
		return
	}

	// Run the task function with the provided payload and deadline
	child, cancel := context.WithDeadline(ctx, (*task.DiesAt).UTC())
	exec.wg.Go(func() {
		defer cancel()

		// Otel span
		spanCtx, endSpan := otel.StartSpan(exec.tracer, child, strings.Join([]string{"queue", task.Queue}, "."),
			attribute.String("task", types.Stringify(task)),
		)

		resp := run(spanCtx, fn, task.Payload)
		if resp == nil {
			resp = new(Result)
		}
		resp.Trace = trace.SpanContextFromContext(spanCtx)
		endSpan(resp.Error)
		resp.Queue = task.Queue
		resp.Task = task
		result <- resp
	})
}

func run(ctx context.Context, fn schema.TaskFunc, payload json.RawMessage) (resp *Result) {
	return runWithGrace(ctx, fn, payload, time.Minute)
}

func runWithGrace(ctx context.Context, fn schema.TaskFunc, payload json.RawMessage, grace time.Duration) (resp *Result) {
	result := make(chan *Result, 1)
	go func() {
		result <- runCallback(ctx, fn, payload)
	}()

	select {
	case resp := <-result:
		return resp
	case <-ctx.Done():
		// TTL is cooperative via context cancellation. Give callbacks an
		// additional grace window to exit before force-failing the task.
		if grace <= 0 {
			return types.Ptr(Result{Error: context.DeadlineExceeded})
		}

		timer := time.NewTimer(grace)
		defer timer.Stop()

		select {
		case resp := <-result:
			return resp
		case <-timer.C:
			return types.Ptr(Result{Error: fmt.Errorf("task exceeded TTL and did not stop after %s grace period: %w", grace, context.DeadlineExceeded)})
		}
	}
}

func runCallback(ctx context.Context, fn schema.TaskFunc, payload json.RawMessage) (resp *Result) {
	defer func() {
		if recovered := recover(); recovered != nil {
			var err error
			switch value := recovered.(type) {
			case error:
				err = value
			default:
				err = fmt.Errorf("panic: %v", value)
			}
			fmt.Fprintf(os.Stderr, "RunQueueTask panic: %v\n%s\n", err, debug.Stack())
			resp = types.Ptr(Result{Error: err})
		}
	}()

	// Execute the method
	result, err := fn(ctx, payload)
	if err != nil {
		return types.Ptr(Result{Error: err})
	}

	// Convert the result to JSON
	data, err := json.Marshal(result)
	if err != nil {
		return types.Ptr(Result{Error: err})
	}

	// Return the result
	return types.Ptr(Result{Result: data})
}
