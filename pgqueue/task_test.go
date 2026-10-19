package pgqueue_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	pgqueue "github.com/mutablelogic/go-pg/pgqueue"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	test "github.com/mutablelogic/go-pg/pgqueue/test"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// taskManager returns an isolated manager with a queue with the meta
func taskManager(t *testing.T, queue string, meta schema.QueueMeta) *pgqueue.Manager {
	t.Helper()
	_, ctx := test.Begin(t)
	manager := test.IsolatedManager(t)
	_, err := manager.RegisterQueue(ctx, queue, meta, noopTask)
	require.NoError(t, err)
	return manager
}

// payload returns a JSON payload
func payload(v string) schema.TaskMeta {
	return schema.TaskMeta{Payload: json.RawMessage(v)}
}

func TestCreateTask(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)

	t.Run("Create", func(t *testing.T) {
		manager := taskManager(t, "create", schema.QueueMeta{})
		task, err := manager.CreateTask(ctx, "create", payload(`{"kind":"create"}`))
		require.NoError(t, err)
		assert.NotZero(t, task.Id)
		assert.Equal(t, "create", task.Queue)
		assert.JSONEq(t, `{"kind":"create"}`, string(task.Payload))
		assert.Equal(t, types.Ptr(uint64(3)), task.Retries)
		assert.Nil(t, task.StartedAt)
		assert.Nil(t, task.DiesAt) // only set when a worker retains the task
	})

	t.Run("Delayed", func(t *testing.T) {
		manager := taskManager(t, "delayed", schema.QueueMeta{})
		later := time.Now().Add(time.Hour)
		task, err := manager.CreateTask(ctx, "delayed", schema.TaskMeta{Payload: json.RawMessage(`{}`), DelayedAt: &later})
		require.NoError(t, err)
		require.NotNil(t, task.DelayedAt)
		assert.WithinDuration(t, later, *task.DelayedAt, time.Second)
	})

	t.Run("Dedupe", func(t *testing.T) {
		// A queue which deduplicates tasks returns the same pending task
		manager := taskManager(t, "dedupe", schema.QueueMeta{Dedupe: types.Ptr(true)})
		first, err := manager.CreateTask(ctx, "dedupe", payload(`{"kind":"dedupe"}`))
		require.NoError(t, err)
		second, err := manager.CreateTask(ctx, "dedupe", payload(`{"kind":"dedupe"}`))
		require.NoError(t, err)
		assert.Equal(t, first.Id, second.Id)
	})

	t.Run("DedupeNotify", func(t *testing.T) {
		// Returning the same pending task notifies the workers, as a worker
		// which polled while the task was locked skipped it
		manager := taskManager(t, "notify", schema.QueueMeta{Dedupe: types.Ptr(true)})
		_, err := manager.CreateTask(ctx, "notify", payload(`{"kind":"notify"}`))
		require.NoError(t, err)

		notifyCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		notifications, err := manager.Subscribe(notifyCtx, schema.DefaultNotifyChannel)
		require.NoError(t, err)
		_, err = manager.CreateTask(ctx, "notify", payload(`{"kind":"notify"}`))
		require.NoError(t, err)

		timeout := time.After(10 * time.Second)
		for {
			select {
			case notification, ok := <-notifications:
				require.True(t, ok, "notifications closed")
				var payload struct {
					Schema string `json:"schema"`
					Queue  string `json:"queue"`
				}
				require.NoError(t, json.Unmarshal(notification.Payload, &payload))
				if payload.Queue == "notify" {
					return
				}
			case <-timeout:
				t.Fatal("timed out waiting for a notification")
			}
		}
	})

	t.Run("Invalid", func(t *testing.T) {
		manager := taskManager(t, "invalid", schema.QueueMeta{})
		_, err := manager.CreateTask(ctx, "missing", payload(`{}`))
		assert.Error(t, err)
		_, err = manager.CreateTask(ctx, "invalid", schema.TaskMeta{})
		assert.ErrorIs(t, err, httpresponse.ErrBadRequest)
	})
}

func TestNextTask(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)

	t.Run("Retain", func(t *testing.T) {
		// The oldest task is retained by the worker, until its TTL
		manager := taskManager(t, "retain", schema.QueueMeta{TTL: types.Ptr(time.Minute)})
		first, err := manager.CreateTask(ctx, "retain", payload(`{"n":1}`))
		require.NoError(t, err)
		_, err = manager.CreateTask(ctx, "retain", payload(`{"n":2}`))
		require.NoError(t, err)

		task, err := manager.NextTask(ctx, "worker")
		require.NoError(t, err)
		require.NotNil(t, task)
		assert.Equal(t, first.Id, task.Id)
		assert.Equal(t, "worker", *task.Worker)
		require.NotNil(t, task.StartedAt)
		require.NotNil(t, task.DiesAt)
		assert.WithinDuration(t, task.StartedAt.Add(time.Minute), *task.DiesAt, time.Second)
	})

	t.Run("Empty", func(t *testing.T) {
		manager := taskManager(t, "empty", schema.QueueMeta{})
		task, err := manager.NextTask(ctx, "worker")
		require.NoError(t, err)
		assert.Nil(t, task)

		_, err = manager.NextTask(ctx, " ")
		assert.ErrorIs(t, err, httpresponse.ErrBadRequest)
	})

	t.Run("Queues", func(t *testing.T) {
		// Only tasks in the queues are retained, or in any queue without
		// queues
		manager := taskManager(t, "one", schema.QueueMeta{})
		_, err := manager.RegisterQueue(ctx, "two", schema.QueueMeta{}, noopTask)
		require.NoError(t, err)
		task, err := manager.CreateTask(ctx, "two", payload(`{}`))
		require.NoError(t, err)

		next, err := manager.NextTask(ctx, "worker", "one")
		require.NoError(t, err)
		assert.Nil(t, next)
		next, err = manager.NextTask(ctx, "worker")
		require.NoError(t, err)
		require.NotNil(t, next)
		assert.Equal(t, task.Id, next.Id)
	})

	t.Run("Delayed", func(t *testing.T) {
		// A delayed task isn't retained until its delay has passed
		manager := taskManager(t, "delayed", schema.QueueMeta{})
		later := time.Now().Add(time.Hour)
		task, err := manager.CreateTask(ctx, "delayed", schema.TaskMeta{Payload: json.RawMessage(`{}`), DelayedAt: &later})
		require.NoError(t, err)
		next, err := manager.NextTask(ctx, "worker")
		require.NoError(t, err)
		assert.Nil(t, next)

		require.NoError(t, manager.With("tid", task.Id).Exec(ctx, `UPDATE ${"schema"}."task" SET "delayed_at" = NOW() - INTERVAL '1 second' WHERE "id" = @tid`))
		next, err = manager.NextTask(ctx, "worker")
		require.NoError(t, err)
		require.NotNil(t, next)
		assert.Equal(t, task.Id, next.Id)
	})

	t.Run("Concurrency", func(t *testing.T) {
		// No more tasks than the queue's concurrency are retained at once
		manager := taskManager(t, "concurrency", schema.QueueMeta{Concurrency: types.Ptr(uint64(1))})
		for range 2 {
			_, err := manager.CreateTask(ctx, "concurrency", payload(`{}`))
			require.NoError(t, err)
		}
		first, err := manager.NextTask(ctx, "worker")
		require.NoError(t, err)
		require.NotNil(t, first)
		next, err := manager.NextTask(ctx, "worker")
		require.NoError(t, err)
		assert.Nil(t, next)

		// Once it's released, the next task is retained
		_, err = manager.ReleaseTask(ctx, first.Id, true, nil, nil)
		require.NoError(t, err)
		next, err = manager.NextTask(ctx, "worker")
		require.NoError(t, err)
		assert.NotNil(t, next)
	})

	t.Run("Unlimited", func(t *testing.T) {
		// A concurrency of zero is unlimited
		manager := taskManager(t, "unlimited", schema.QueueMeta{Concurrency: types.Ptr(uint64(0))})
		for range 3 {
			_, err := manager.CreateTask(ctx, "unlimited", payload(`{}`))
			require.NoError(t, err)
		}
		for range 3 {
			next, err := manager.NextTask(ctx, "worker")
			require.NoError(t, err)
			assert.NotNil(t, next)
		}
	})

	t.Run("Expired", func(t *testing.T) {
		// A task which ran past its TTL is retained again, and doesn't count
		// towards the queue's concurrency
		manager := taskManager(t, "expired", schema.QueueMeta{Concurrency: types.Ptr(uint64(1))})
		task, err := manager.CreateTask(ctx, "expired", payload(`{}`))
		require.NoError(t, err)
		next, err := manager.NextTask(ctx, "first")
		require.NoError(t, err)
		require.NotNil(t, next)
		require.NoError(t, manager.With("tid", task.Id).Exec(ctx, `UPDATE ${"schema"}."task" SET "dies_at" = NOW() - INTERVAL '1 second' WHERE "id" = @tid`))

		next, err = manager.NextTask(ctx, "second")
		require.NoError(t, err)
		require.NotNil(t, next)
		assert.Equal(t, task.Id, next.Id)
		assert.Equal(t, "second", *next.Worker)
	})

	t.Run("NoRetries", func(t *testing.T) {
		// A task in a queue without retries is failed, and isn't retained
		manager := taskManager(t, "noretries", schema.QueueMeta{Retries: types.Ptr(uint64(0))})
		task, err := manager.CreateTask(ctx, "noretries", payload(`{}`))
		require.NoError(t, err)
		next, err := manager.NextTask(ctx, "worker")
		require.NoError(t, err)
		assert.Nil(t, next)

		list, err := manager.ListTasks(ctx, schema.TaskListRequest{Status: "failed"})
		require.NoError(t, err)
		require.Len(t, list.Body, 1)
		assert.Equal(t, task.Id, list.Body[0].Id)
	})
}

func TestReleaseTask(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)

	t.Run("Done", func(t *testing.T) {
		manager := taskManager(t, "done", schema.QueueMeta{})
		_, err := manager.CreateTask(ctx, "done", payload(`{}`))
		require.NoError(t, err)
		next, err := manager.NextTask(ctx, "worker")
		require.NoError(t, err)

		var status string
		task, err := manager.ReleaseTask(ctx, next.Id, true, json.RawMessage(`{"ok":true}`), &status)
		require.NoError(t, err)
		assert.Equal(t, "done", status)
		assert.JSONEq(t, `{"ok":true}`, string(task.Result))
		assert.NotNil(t, task.FinishedAt)
		assert.Nil(t, task.DiesAt)
	})

	t.Run("Retry", func(t *testing.T) {
		// A failed task is retried after a backoff, until it has no retries
		// left
		manager := taskManager(t, "retry", schema.QueueMeta{Retries: types.Ptr(uint64(2)), RetryDelay: types.Ptr(time.Minute)})
		_, err := manager.CreateTask(ctx, "retry", payload(`{}`))
		require.NoError(t, err)
		next, err := manager.NextTask(ctx, "worker")
		require.NoError(t, err)

		var status string
		task, err := manager.ReleaseTask(ctx, next.Id, false, json.RawMessage(`"boom"`), &status)
		require.NoError(t, err)
		assert.Equal(t, "retry", status)
		assert.Equal(t, types.Ptr(uint64(1)), task.Retries)
		assert.JSONEq(t, `"boom"`, string(task.Result))
		require.NotNil(t, task.DelayedAt)
		assert.WithinDuration(t, time.Now().Add(time.Minute), *task.DelayedAt, 10*time.Second)

		// It fails with no retries left
		require.NoError(t, manager.With("tid", task.Id).Exec(ctx, `UPDATE ${"schema"}."task" SET "delayed_at" = NULL WHERE "id" = @tid`))
		next, err = manager.NextTask(ctx, "worker")
		require.NoError(t, err)
		require.NotNil(t, next)
		_, err = manager.ReleaseTask(ctx, next.Id, false, json.RawMessage(`"boom"`), &status)
		require.NoError(t, err)
		assert.Equal(t, "failed", status)
	})

	t.Run("NotRunning", func(t *testing.T) {
		// Only a running task can be released
		manager := taskManager(t, "notrunning", schema.QueueMeta{})
		task, err := manager.CreateTask(ctx, "notrunning", payload(`{}`))
		require.NoError(t, err)
		_, err = manager.ReleaseTask(ctx, task.Id, true, nil, nil)
		assert.ErrorIs(t, err, pg.ErrNotFound)
		_, err = manager.ReleaseTask(ctx, 999999, true, nil, nil)
		assert.ErrorIs(t, err, pg.ErrNotFound)
		_, err = manager.ReleaseTask(ctx, 0, true, nil, nil)
		assert.ErrorIs(t, err, httpresponse.ErrBadRequest)
	})
}

func TestGetTask(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := taskManager(t, "get", schema.QueueMeta{})
	created, err := manager.CreateTask(ctx, "get", payload(`{"n":1}`))
	require.NoError(t, err)

	t.Run("New", func(t *testing.T) {
		task, err := manager.GetTask(ctx, created.Id)
		require.NoError(t, err)
		assert.Equal(t, created.Id, task.Id)
		assert.Equal(t, "get", task.Queue)
		assert.Equal(t, "new", task.Status)
		assert.Nil(t, task.Worker)
	})

	t.Run("NotRetained", func(t *testing.T) {
		// Getting the task doesn't retain it, so it's still the next task,
		// and it's then running
		next, err := manager.NextTask(ctx, "worker")
		require.NoError(t, err)
		require.NotNil(t, next)
		assert.Equal(t, created.Id, next.Id)

		task, err := manager.GetTask(ctx, created.Id)
		require.NoError(t, err)
		assert.Equal(t, "running", task.Status)
	})

	t.Run("NotFound", func(t *testing.T) {
		_, err := manager.GetTask(ctx, created.Id+1000)
		assert.ErrorIs(t, err, pg.ErrNotFound)
	})
}

func TestListTasks(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := taskManager(t, "list", schema.QueueMeta{})
	for i := range 3 {
		_, err := manager.CreateTask(ctx, "list", payload(`{"n":`+strconv.Itoa(i)+`}`))
		require.NoError(t, err)
	}
	next, err := manager.NextTask(ctx, "worker")
	require.NoError(t, err)

	t.Run("All", func(t *testing.T) {
		list, err := manager.ListTasks(ctx, schema.TaskListRequest{})
		require.NoError(t, err)
		assert.EqualValues(t, 3, list.Count)
		assert.Len(t, list.Body, 3)

		// The default limit is returned, clamped to the tasks available
		require.NotNil(t, list.Limit)
		assert.EqualValues(t, 3, *list.Limit)
	})

	t.Run("Status", func(t *testing.T) {
		list, err := manager.ListTasks(ctx, schema.TaskListRequest{Status: "running"})
		require.NoError(t, err)
		assert.EqualValues(t, 1, list.Count)
		require.Len(t, list.Body, 1)
		assert.Equal(t, next.Id, list.Body[0].Id)
		assert.Equal(t, "running", list.Body[0].Status)

		list, err = manager.ListTasks(ctx, schema.TaskListRequest{Status: "new"})
		require.NoError(t, err)
		assert.EqualValues(t, 2, list.Count)
	})

	t.Run("Page", func(t *testing.T) {
		limit := types.Ptr(uint64(10))
		list, err := manager.ListTasks(ctx, schema.TaskListRequest{OffsetLimit: pg.OffsetLimit{Offset: 2, Limit: limit}})
		require.NoError(t, err)
		assert.EqualValues(t, 3, list.Count)
		assert.Len(t, list.Body, 1)
		require.NotNil(t, list.Limit)
		assert.EqualValues(t, 1, *list.Limit)

		// The caller's limit isn't changed
		assert.EqualValues(t, 10, *limit)
	})
}
