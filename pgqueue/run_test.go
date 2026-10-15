package pgqueue_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	pgqueue "github.com/mutablelogic/go-pg/pgqueue"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	test "github.com/mutablelogic/go-pg/pgqueue/test"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	logger := slog.New(slog.DiscardHandler)

	t.Run("Cancel", func(t *testing.T) {
		// The event loop runs until its context is cancelled
		manager := test.IsolatedManager(t)
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- manager.Run(runCtx, logger)
		}()
		require.Eventually(t, manager.Running, 10*time.Second, 10*time.Millisecond)

		cancel()
		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("the event loop didn't stop")
		}
		assert.False(t, manager.Running())
	})

	t.Run("AlreadyRunning", func(t *testing.T) {
		// Only one event loop can run at a time
		manager, _ := test.Begin(t)
		assert.ErrorIs(t, manager.Run(ctx, logger), pg.ErrConflict)
	})
}

// runIsolated runs the event loop of the manager, and stops it when the test
// ends, or when the returned function is called
func runIsolated(t *testing.T, manager *pgqueue.Manager) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- manager.Run(ctx, slog.New(slog.DiscardHandler))
	}()
	require.Eventually(t, manager.Running, 10*time.Second, 10*time.Millisecond)

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			assert.NoError(t, <-done)
		})
	}
	t.Cleanup(stop)
	return stop
}

// waitStatus waits for the task to have the status, and returns it
func waitStatus(t *testing.T, ctx context.Context, manager *pgqueue.Manager, id uint64, status string) schema.TaskWithStatus {
	t.Helper()
	var task schema.TaskWithStatus
	require.Eventually(t, func() bool {
		err := manager.Get(ctx, &task, schema.TaskId(id))
		return err == nil && task.Status == status
	}, 10*time.Second, 10*time.Millisecond, "task %d isn't %q", id, status)
	return task
}

func TestRunQueues(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)

	t.Run("Done", func(t *testing.T) {
		// A task's result is released when it's done, and the task is in
		// its context
		manager := test.IsolatedManager(t)
		_, err := manager.RegisterQueue(ctx, "done", schema.QueueMeta{}, func(ctx context.Context, payload json.RawMessage) (any, error) {
			task := schema.TaskFromContext(ctx)
			if task == nil {
				return nil, errors.New("no task in the context")
			}
			return map[string]any{"payload": payload, "id": task.Id}, nil
		})
		require.NoError(t, err)
		runIsolated(t, manager)

		task, err := manager.CreateTask(ctx, "done", schema.TaskMeta{Payload: json.RawMessage(`{"n":1}`)})
		require.NoError(t, err)
		done := waitStatus(t, ctx, manager, task.Id, "done")
		assert.JSONEq(t, `{"payload":{"n":1},"id":`+strconv.FormatUint(task.Id, 10)+`}`, string(done.Result))
	})

	t.Run("Failed", func(t *testing.T) {
		// A task's error is its result when it fails
		manager := test.IsolatedManager(t)
		_, err := manager.RegisterQueue(ctx, "failed", schema.QueueMeta{Retries: types.Ptr(uint64(1))}, func(context.Context, json.RawMessage) (any, error) {
			return nil, errors.New("boom")
		})
		require.NoError(t, err)
		runIsolated(t, manager)

		task, err := manager.CreateTask(ctx, "failed", schema.TaskMeta{Payload: json.RawMessage(`{}`)})
		require.NoError(t, err)
		failed := waitStatus(t, ctx, manager, task.Id, "failed")
		assert.JSONEq(t, `"boom"`, string(failed.Result))
	})

	t.Run("Concurrency", func(t *testing.T) {
		// No more tasks than the queue's concurrency run at once
		manager := test.IsolatedManager(t)
		started := make(chan struct{}, 2)
		release := make(chan struct{})
		_, err := manager.RegisterQueue(ctx, "concurrency", schema.QueueMeta{Concurrency: types.Ptr(uint64(1))}, func(context.Context, json.RawMessage) (any, error) {
			started <- struct{}{}
			<-release
			return nil, nil
		})
		require.NoError(t, err)
		runIsolated(t, manager)

		for i := range 2 {
			_, err := manager.CreateTask(ctx, "concurrency", schema.TaskMeta{Payload: json.RawMessage(`{"n":` + strconv.Itoa(i) + `}`)})
			require.NoError(t, err)
		}
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("the first task didn't start")
		}
		select {
		case <-started:
			t.Fatal("the second task started with the first running")
		case <-time.After(300 * time.Millisecond):
		}

		// The second runs once the first is done
		release <- struct{}{}
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("the second task didn't start")
		}
		close(release)
	})

	t.Run("OtherQueues", func(t *testing.T) {
		// The tasks of a queue without a callback in this process, such as
		// one registered by another process, aren't run
		manager := test.IsolatedManager(t)
		var other schema.Queue
		require.NoError(t, manager.Insert(ctx, &other, schema.Queue{Queue: "other"}))
		otherTask, err := manager.CreateTask(ctx, "other", schema.TaskMeta{Payload: json.RawMessage(`{}`)})
		require.NoError(t, err)
		_, err = manager.RegisterQueue(ctx, "mine", schema.QueueMeta{}, noopTask)
		require.NoError(t, err)
		runIsolated(t, manager)

		// Once a task of its own queue is done, the other task is still new
		mine, err := manager.CreateTask(ctx, "mine", schema.TaskMeta{Payload: json.RawMessage(`{}`)})
		require.NoError(t, err)
		waitStatus(t, ctx, manager, mine.Id, "done")
		waitStatus(t, ctx, manager, otherTask.Id, "new")
	})

	t.Run("Stop", func(t *testing.T) {
		// A running task is put back when the loop stops, without using a
		// retry
		manager := test.IsolatedManager(t)
		started := make(chan struct{}, 1)
		_, err := manager.RegisterQueue(ctx, "stop", schema.QueueMeta{}, func(ctx context.Context, _ json.RawMessage) (any, error) {
			started <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		})
		require.NoError(t, err)
		stop := runIsolated(t, manager)

		task, err := manager.CreateTask(ctx, "stop", schema.TaskMeta{Payload: json.RawMessage(`{}`)})
		require.NoError(t, err)
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("the task didn't start")
		}
		stop()

		requeued := waitStatus(t, ctx, manager, task.Id, "new")
		assert.Equal(t, types.Ptr(uint64(3)), requeued.Retries)
		assert.Nil(t, requeued.Worker)
		assert.Nil(t, requeued.StartedAt)
		assert.Nil(t, requeued.DiesAt)
	})
}

func TestRunTickers(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)

	// ticked returns a callback which sends the ticker's name when it runs
	ticked := func(ch chan<- string, name string) schema.TaskFunc {
		return func(context.Context, json.RawMessage) (any, error) {
			ch <- name
			return nil, nil
		}
	}

	t.Run("Due", func(t *testing.T) {
		// Every due ticker runs straight away, rather than one each poll
		manager := test.IsolatedManager(t)
		ch := make(chan string, 3)
		for _, name := range []string{"one", "two", "three"} {
			_, err := manager.RegisterTicker(ctx, name, schema.TickerMeta{Interval: types.Ptr(time.Hour)}, ticked(ch, name))
			require.NoError(t, err)
		}
		runIsolated(t, manager)

		var ran []string
		for range 3 {
			select {
			case name := <-ch:
				ran = append(ran, name)
			case <-time.After(schema.DefaultTickerPeriod / 2):
				t.Fatalf("only %q ran before the next poll", ran)
			}
		}
		assert.ElementsMatch(t, []string{"one", "two", "three"}, ran)

		// And then not again until their interval has passed
		select {
		case name := <-ch:
			t.Fatalf("%q ran again", name)
		case <-time.After(300 * time.Millisecond):
		}
	})

	t.Run("BuiltIn", func(t *testing.T) {
		// The maintenance and cleanup tickers run
		manager := test.IsolatedManager(t)
		runIsolated(t, manager)
		for _, name := range []string{schema.DefaultMaintenanceTickerName, schema.DefaultCleanupTickerName} {
			require.Eventually(t, func() bool {
				ticker, err := manager.GetTicker(ctx, name)
				return err == nil && ticker.LastAt != nil
			}, 10*time.Second, 10*time.Millisecond, "%q didn't run", name)
		}
	})

	t.Run("OtherTickers", func(t *testing.T) {
		// A ticker without a callback in this process, such as one registered
		// by another process, isn't claimed
		manager := test.IsolatedManager(t)
		var other schema.Ticker
		require.NoError(t, manager.With("id", "other").Insert(ctx, &other, schema.TickerMeta{}))
		ch := make(chan string, 1)
		_, err := manager.RegisterTicker(ctx, "mine", schema.TickerMeta{Interval: types.Ptr(time.Hour)}, ticked(ch, "mine"))
		require.NoError(t, err)
		runIsolated(t, manager)

		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatal("the ticker didn't run")
		}
		ticker, err := manager.GetTicker(ctx, "other")
		require.NoError(t, err)
		assert.Nil(t, ticker.LastAt)
	})
}

func TestRunCleanup(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)

	// More finished tasks than are cleaned at once, which aren't retained
	manager := test.IsolatedManager(t, pgqueue.WithRetention(0))
	_, err := manager.RegisterQueue(ctx, "cleanup", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)
	count := schema.QueueCleanLimit + schema.QueueCleanLimit/2
	for i := range count {
		_, err := manager.CreateTask(ctx, "cleanup", schema.TaskMeta{Payload: json.RawMessage(`{"n":` + strconv.Itoa(i) + `}`)})
		require.NoError(t, err)
	}
	require.NoError(t, manager.Exec(ctx, `UPDATE ${"schema"}."task" SET "started_at" = NOW() - INTERVAL '1 second', "finished_at" = NOW() - INTERVAL '1 second'`))

	// The cleanup ticker runs when the loop starts, and removes them all
	runIsolated(t, manager)
	require.Eventually(t, func() bool {
		list, err := manager.ListTasks(ctx, schema.TaskListRequest{})
		return err == nil && list.Count == 0
	}, 10*time.Second, 10*time.Millisecond)
}
