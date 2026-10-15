package pgqueue

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestRegisterTask(t *testing.T) {
	registry := newExec(nil)
	fn := func(context.Context, json.RawMessage) (any, error) { return nil, nil }

	err := registry.RegisterTask("  Example_Task  ", fn)
	require.NoError(t, err)
	if assert.NotNil(t, registry.t) {
		_, exists := registry.t["example_task"]
		assert.True(t, exists)
	}

	err = registry.RegisterTask("example_task", fn)
	require.Error(t, err)
	assert.True(t, errors.Is(err, httpresponse.ErrConflict))
}

func TestRemoveTask(t *testing.T) {
	registry := newExec(nil)
	fn := func(context.Context, json.RawMessage) (any, error) { return nil, nil }

	require.NoError(t, registry.RegisterTask("remove_task", fn))
	require.NoError(t, registry.RemoveTask("remove_task"))

	err := registry.RemoveTask("remove_task")
	require.Error(t, err)
	assert.True(t, errors.Is(err, httpresponse.ErrNotFound))
}

func TestRunRecoversPanic(t *testing.T) {
	result := run(context.Background(), func(context.Context, json.RawMessage) (any, error) {
		panic("boom")
	}, nil)

	require.NotNil(t, result)
	require.Error(t, result.Error)
	assert.EqualError(t, result.Error, "panic: boom")
	assert.Nil(t, result.Result)
}

func TestRunTickerTaskKeepsContextAlive(t *testing.T) {
	exec := newExec(nil)
	results := make(chan *Result, 1)
	ticker := &schema.Ticker{Ticker: "alive_context"}

	require.NoError(t, exec.RegisterTask(ticker.Ticker, func(ctx context.Context, _ json.RawMessage) (any, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return map[string]bool{"ok": true}, nil
		}
	}))

	require.NoError(t, exec.RunTickerTask(context.Background(), ticker, results))

	select {
	case result := <-results:
		require.NotNil(t, result)
		require.NoError(t, result.Error)
		assert.Equal(t, ticker.Ticker, result.Ticker)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ticker result")
	}

	exec.Close()
}

func TestRunQueueTaskUsesTaskTTLDeadline(t *testing.T) {
	exec := newExec(nil)
	results := make(chan *Result, 1)
	diesAt := time.Now().Add(2 * time.Second)
	task := &schema.Task{Id: 42, Queue: "queue_deadline", DiesAt: &diesAt}

	require.NoError(t, exec.RegisterTask(task.Queue, func(ctx context.Context, _ json.RawMessage) (any, error) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		assert.WithinDuration(t, diesAt.UTC(), deadline.UTC(), 50*time.Millisecond)
		return map[string]bool{"ok": true}, nil
	}))

	exec.RunQueueTask(context.Background(), task, results)

	select {
	case result := <-results:
		require.NotNil(t, result)
		require.NoError(t, result.Error)
		assert.Equal(t, task.Queue, result.Queue)
		if assert.NotNil(t, result.Task) {
			assert.Equal(t, task.Id, result.Task.Id)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queue task result")
	}

	exec.Close()
}

func TestRunQueueTaskRequiresTaskDeadline(t *testing.T) {
	exec := newExec(nil)
	results := make(chan *Result, 1)
	task := &schema.Task{Id: 99, Queue: "queue_deadline_required"}
	called := false

	require.NoError(t, exec.RegisterTask(task.Queue, func(ctx context.Context, _ json.RawMessage) (any, error) {
		called = true
		return nil, nil
	}))

	exec.RunQueueTask(context.Background(), task, results)
	assert.False(t, called)

	select {
	case result := <-results:
		require.NotNil(t, result)
		require.Error(t, result.Error)
		assert.True(t, errors.Is(result.Error, httpresponse.ErrBadRequest))
		assert.Equal(t, task.Queue, result.Queue)
		if assert.NotNil(t, result.Task) {
			assert.Equal(t, task.Id, result.Task.Id)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queue task error result")
	}

	exec.Close()
}

func TestRunReturnsOnContextDeadlineWhenCallbackBlocks(t *testing.T) {
	unblock := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	result := runWithGrace(ctx, func(context.Context, json.RawMessage) (any, error) {
		<-unblock
		return map[string]bool{"ok": true}, nil
	}, nil, 20*time.Millisecond)
	elapsed := time.Since(start)
	close(unblock)

	require.NotNil(t, result)
	require.Error(t, result.Error)
	assert.True(t, errors.Is(result.Error, context.DeadlineExceeded))
	assert.Less(t, elapsed, 500*time.Millisecond)
}

func TestRunReturnsCallbackErrorIfItExitsDuringGrace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	result := runWithGrace(ctx, func(ctx context.Context, _ json.RawMessage) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, nil, 100*time.Millisecond)
	elapsed := time.Since(start)

	require.NotNil(t, result)
	require.Error(t, result.Error)
	assert.True(t, errors.Is(result.Error, context.DeadlineExceeded))
	assert.Less(t, elapsed, 500*time.Millisecond)
}

func TestRunWithGracePreservesCanceledCause(t *testing.T) {
	unblock := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	start := time.Now()
	go func() {
		time.Sleep(25 * time.Millisecond)
		cancel()
	}()

	result := runWithGrace(ctx, func(context.Context, json.RawMessage) (any, error) {
		<-unblock
		return map[string]bool{"ok": true}, nil
	}, nil, 20*time.Millisecond)
	elapsed := time.Since(start)
	close(unblock)

	require.NotNil(t, result)
	require.Error(t, result.Error)
	assert.True(t, errors.Is(result.Error, context.Canceled))
	assert.Less(t, elapsed, 500*time.Millisecond)
}

func TestRunTaskContext(t *testing.T) {
	t.Run("Ticker", func(t *testing.T) {
		// A ticker's callback has the ticker in its context
		exec := newExec(nil)
		results := make(chan *Result, 1)
		ticker := &schema.Ticker{Ticker: "context_ticker"}
		var got *schema.Ticker
		require.NoError(t, exec.RegisterTask(ticker.Ticker, func(ctx context.Context, _ json.RawMessage) (any, error) {
			got = schema.TickerFromContext(ctx)
			return nil, nil
		}))
		require.NoError(t, exec.RunTickerTask(context.Background(), ticker, results))
		<-results
		exec.Close()
		assert.Same(t, ticker, got)
	})

	t.Run("Task", func(t *testing.T) {
		// A task's callback has the task in its context
		exec := newExec(nil)
		results := make(chan *Result, 1)
		diesAt := time.Now().Add(time.Minute)
		task := &schema.Task{Id: 42, Queue: "context_queue", DiesAt: &diesAt}
		var got *schema.Task
		require.NoError(t, exec.RegisterTask(task.Queue, func(ctx context.Context, _ json.RawMessage) (any, error) {
			got = schema.TaskFromContext(ctx)
			return nil, nil
		}))
		exec.RunQueueTask(context.Background(), task, results)
		<-results
		exec.Close()
		assert.Same(t, task, got)
		assert.Nil(t, schema.TickerFromContext(context.Background()))
	})
}
