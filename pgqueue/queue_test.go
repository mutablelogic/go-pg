package pgqueue_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"strings"
	"sync"
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

// noopTask is a task callback which does nothing
func noopTask(context.Context, json.RawMessage) (any, error) {
	return nil, nil
}

func TestRegisterQueue(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)

	t.Run("Defaults", func(t *testing.T) {
		manager := test.IsolatedManager(t)
		queue, err := manager.RegisterQueue(ctx, "defaults", schema.QueueMeta{}, noopTask)
		require.NoError(t, err)
		assert.Equal(t, schema.Queue{Queue: "defaults", QueueMeta: schema.QueueMeta{
			TTL:         types.Ptr(time.Hour),
			Retries:     types.Ptr(uint64(3)),
			RetryDelay:  types.Ptr(2 * time.Minute),
			Concurrency: types.Ptr(uint64(0)),
			Dedupe:      types.Ptr(false),
		}}, *queue)
	})

	t.Run("Meta", func(t *testing.T) {
		manager := test.IsolatedManager(t)
		meta := schema.QueueMeta{
			TTL:         types.Ptr(3 * time.Hour),
			Retries:     types.Ptr(uint64(7)),
			RetryDelay:  types.Ptr(5 * time.Minute),
			Concurrency: types.Ptr(uint64(2)),
			Dedupe:      types.Ptr(true),
		}
		queue, err := manager.RegisterQueue(ctx, "meta", meta, noopTask)
		require.NoError(t, err)
		assert.Equal(t, schema.Queue{Queue: "meta", QueueMeta: meta}, *queue)
	})

	t.Run("Name", func(t *testing.T) {
		// The name is lowercased and trimmed
		manager := test.IsolatedManager(t)
		queue, err := manager.RegisterQueue(ctx, "  Mixed_Case  ", schema.QueueMeta{}, noopTask)
		require.NoError(t, err)
		assert.Equal(t, "mixed_case", queue.Queue)
	})

	t.Run("Existing", func(t *testing.T) {
		// A queue which exists, such as one registered by another process,
		// is updated with the meta
		manager := test.IsolatedManager(t)
		var existing schema.Queue
		require.NoError(t, manager.Insert(ctx, &existing, schema.Queue{Queue: "existing"}))
		queue, err := manager.RegisterQueue(ctx, "existing", schema.QueueMeta{Retries: types.Ptr(uint64(5))}, noopTask)
		require.NoError(t, err)
		assert.Equal(t, uint64(5), *queue.Retries)
		assert.Equal(t, time.Hour, *queue.TTL)
	})

	t.Run("Twice", func(t *testing.T) {
		// A queue's callback can only be registered once
		manager := test.IsolatedManager(t)
		_, err := manager.RegisterQueue(ctx, "twice", schema.QueueMeta{}, noopTask)
		require.NoError(t, err)
		_, err = manager.RegisterQueue(ctx, "twice", schema.QueueMeta{Retries: types.Ptr(uint64(5))}, noopTask)
		assert.ErrorIs(t, err, httpresponse.ErrConflict)

		// And the queue isn't changed
		queue, err := manager.GetQueue(ctx, "twice")
		require.NoError(t, err)
		assert.Equal(t, uint64(3), *queue.Retries)
	})

	t.Run("Invalid", func(t *testing.T) {
		manager := test.IsolatedManager(t)
		_, err := manager.RegisterQueue(ctx, "no callback", schema.QueueMeta{}, noopTask)
		assert.ErrorIs(t, err, httpresponse.ErrBadRequest)
		_, err = manager.RegisterQueue(ctx, "nocallback", schema.QueueMeta{}, nil)
		assert.ErrorIs(t, err, httpresponse.ErrBadRequest)

		// Neither queue is created
		list, err := manager.ListQueues(ctx, schema.QueueListRequest{})
		require.NoError(t, err)
		assert.Zero(t, list.Count)
	})
}

func TestListQueues(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := test.IsolatedManager(t)
	for _, name := range []string{"two", "one", "three"} {
		_, err := manager.RegisterQueue(ctx, name, schema.QueueMeta{}, noopTask)
		require.NoError(t, err)
	}

	t.Run("All", func(t *testing.T) {
		// The queues are in order of name
		list, err := manager.ListQueues(ctx, schema.QueueListRequest{})
		require.NoError(t, err)
		assert.EqualValues(t, 3, list.Count)
		names := make([]string, len(list.Body))
		for i, queue := range list.Body {
			names[i] = queue.Queue
		}
		assert.Equal(t, []string{"one", "three", "two"}, names)
	})

	t.Run("Page", func(t *testing.T) {
		list, err := manager.ListQueues(ctx, schema.QueueListRequest{OffsetLimit: pg.OffsetLimit{Offset: 1, Limit: types.Ptr(uint64(10))}})
		require.NoError(t, err)
		assert.EqualValues(t, 3, list.Count)
		require.Len(t, list.Body, 2)
		assert.Equal(t, "three", list.Body[0].Queue)
		require.NotNil(t, list.Limit)
		assert.EqualValues(t, 2, *list.Limit)
	})
}

func TestGetUpdateDeleteQueue(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := test.IsolatedManager(t)
	_, err := manager.RegisterQueue(ctx, "queue", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)

	t.Run("Get", func(t *testing.T) {
		queue, err := manager.GetQueue(ctx, "queue")
		require.NoError(t, err)
		assert.Equal(t, "queue", queue.Queue)

		_, err = manager.GetQueue(ctx, "missing")
		assert.ErrorIs(t, err, pg.ErrNotFound)
	})

	t.Run("Update", func(t *testing.T) {
		queue, err := manager.UpdateQueue(ctx, "queue", schema.QueueMeta{Retries: types.Ptr(uint64(9)), Dedupe: types.Ptr(true)})
		require.NoError(t, err)
		assert.Equal(t, uint64(9), *queue.Retries)
		assert.True(t, *queue.Dedupe)

		_, err = manager.UpdateQueue(ctx, "missing", schema.QueueMeta{Retries: types.Ptr(uint64(9))})
		assert.ErrorIs(t, err, pg.ErrNotFound)
	})

	t.Run("Delete", func(t *testing.T) {
		queue, err := manager.DeleteQueue(ctx, "queue")
		require.NoError(t, err)
		assert.Equal(t, "queue", queue.Queue)
		_, err = manager.GetQueue(ctx, "queue")
		assert.ErrorIs(t, err, pg.ErrNotFound)

		// The callback is removed, so the queue can be registered again
		_, err = manager.RegisterQueue(ctx, "queue", schema.QueueMeta{}, noopTask)
		require.NoError(t, err)

		_, err = manager.DeleteQueue(ctx, "missing")
		assert.ErrorIs(t, err, pg.ErrNotFound)
	})

	t.Run("DeleteWithoutCallback", func(t *testing.T) {
		// A queue registered by another process has no callback in this one
		var existing schema.Queue
		require.NoError(t, manager.Insert(ctx, &existing, schema.Queue{Queue: "other"}))
		queue, err := manager.DeleteQueue(ctx, "other")
		require.NoError(t, err)
		assert.Equal(t, "other", queue.Queue)
	})
}

func TestCleanQueue(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := test.IsolatedManager(t)
	_, err := manager.RegisterQueue(ctx, "clean", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)

	// A task with each status, set directly
	ids := map[string]uint64{}
	for status, columns := range map[string]string{
		"new":     ``,
		"retry":   `"retries" = "retries" - 1`,
		"running": `"started_at" = NOW(), "dies_at" = NOW() + INTERVAL '1 hour'`,
		"expired": `"started_at" = NOW(), "dies_at" = NOW() - INTERVAL '1 minute'`,
		"failed":  `"retries" = 0`,
		"done":    `"started_at" = NOW(), "finished_at" = NOW()`,
	} {
		var id schema.TaskId
		require.NoError(t, manager.With("id", "clean").Insert(ctx, &id, schema.TaskMeta{Payload: json.RawMessage(`{"status":"` + status + `"}`)}))
		if columns != "" {
			require.NoError(t, manager.With("tid", uint64(id)).Exec(ctx, `UPDATE ${"schema"}."task" SET `+columns+` WHERE "id" = @tid`))
		}
		ids[status] = uint64(id)
	}

	t.Run("Statuses", func(t *testing.T) {
		// Each queue has a count for every status
		statuses, err := manager.ListQueueStatuses(ctx)
		require.NoError(t, err)
		counts := map[string]uint64{}
		for _, status := range statuses {
			assert.Equal(t, "clean", status.Queue)
			counts[status.Status] = status.Count
		}
		assert.Equal(t, map[string]uint64{"new": 1, "retry": 1, "running": 1, "expired": 1, "failed": 1, "done": 1}, counts)
	})

	t.Run("Retained", func(t *testing.T) {
		// Tasks which finished within the retention period are kept
		removed, err := manager.CleanQueue(ctx, "clean")
		require.NoError(t, err)
		assert.Empty(t, removed)
	})

	t.Run("Clean", func(t *testing.T) {
		// The expired, failed and done tasks are removed once they finished
		// longer ago than the retention period
		for _, status := range []string{"expired", "failed", "done"} {
			require.NoError(t, manager.With("tid", ids[status]).Exec(ctx, `UPDATE ${"schema"}."task" SET
				"created_at" = "created_at" - INTERVAL '2 days',
				"delayed_at" = "delayed_at" - INTERVAL '2 days',
				"started_at" = "started_at" - INTERVAL '2 days',
				"finished_at" = "finished_at" - INTERVAL '2 days',
				"dies_at" = "dies_at" - INTERVAL '2 days'
			WHERE "id" = @tid`))
		}
		removed, err := manager.CleanQueue(ctx, "clean")
		require.NoError(t, err)
		var got []uint64
		for _, task := range removed {
			got = append(got, task.Id)
		}
		assert.ElementsMatch(t, []uint64{ids["expired"], ids["failed"], ids["done"]}, got)

		// The other tasks are kept
		statuses, err := manager.ListQueueStatuses(ctx)
		require.NoError(t, err)
		counts := map[string]uint64{}
		for _, status := range statuses {
			counts[status.Status] = status.Count
		}
		assert.Equal(t, map[string]uint64{"new": 1, "retry": 1, "running": 1, "expired": 0, "failed": 0, "done": 0}, counts)
	})
}

func TestUnregisterQueue(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)

	t.Run("Unregister", func(t *testing.T) {
		// The queue and its tasks are kept, and it can be registered again
		manager := test.IsolatedManager(t)
		_, err := manager.RegisterQueue(ctx, "queue", schema.QueueMeta{}, noopTask)
		require.NoError(t, err)
		task, err := manager.CreateTask(ctx, "queue", schema.TaskMeta{Payload: json.RawMessage(`{}`)})
		require.NoError(t, err)

		require.NoError(t, manager.UnregisterQueue(ctx, "queue"))
		_, err = manager.GetQueue(ctx, "queue")
		require.NoError(t, err)
		list, err := manager.ListTasks(ctx, schema.TaskListRequest{Queue: []string{"queue"}})
		require.NoError(t, err)
		require.Len(t, list.Body, 1)
		assert.Equal(t, task.Id, list.Body[0].Id)

		_, err = manager.RegisterQueue(ctx, "queue", schema.QueueMeta{}, noopTask)
		require.NoError(t, err)
	})

	t.Run("NotRegistered", func(t *testing.T) {
		manager := test.IsolatedManager(t)
		assert.ErrorIs(t, manager.UnregisterQueue(ctx, "missing"), httpresponse.ErrNotFound)
	})

	t.Run("NotRun", func(t *testing.T) {
		// The tasks of an unregistered queue aren't run, while another
		// queue's are
		manager := test.IsolatedManager(t)
		for _, name := range []string{"unregistered", "registered"} {
			_, err := manager.RegisterQueue(ctx, name, schema.QueueMeta{}, noopTask)
			require.NoError(t, err)
		}
		require.NoError(t, manager.UnregisterQueue(ctx, "unregistered"))
		runIsolated(t, manager)

		unregistered, err := manager.CreateTask(ctx, "unregistered", schema.TaskMeta{Payload: json.RawMessage(`{}`)})
		require.NoError(t, err)
		registered, err := manager.CreateTask(ctx, "registered", schema.TaskMeta{Payload: json.RawMessage(`{}`)})
		require.NoError(t, err)
		waitStatus(t, ctx, manager, registered.Id, "done")
		waitStatus(t, ctx, manager, unregistered.Id, "new")
	})
}

func TestDeleteQueueAlreadyDeleted(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)

	// A queue which another process deleted first is not found, but its
	// callback is removed, so it can be registered again
	manager := test.IsolatedManager(t)
	_, err := manager.RegisterQueue(ctx, "queue", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)
	require.NoError(t, manager.Exec(ctx, `DELETE FROM ${"schema"}."queue" WHERE "queue" = 'queue'`))

	_, err = manager.DeleteQueue(ctx, "queue")
	assert.ErrorIs(t, err, pg.ErrNotFound)
	_, err = manager.RegisterQueue(ctx, "queue", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)
}

func TestRegisterConcurrently(t *testing.T) {
	manager, ctx := test.Begin(t)
	defer test.End(t)

	// Several instances sharing the database, each with its own callbacks
	instances := make([]*pgqueue.Manager, 5)
	for i := range instances {
		instance, err := pgqueue.New(ctx, manager.PoolConn, pgqueue.WithSchema(test.Schema()))
		require.NoError(t, err)
		instances[i] = instance
	}

	// They all register the same queue and ticker at once
	name := "concurrent_" + strings.ToLower(rand.Text()[:8])
	var wg sync.WaitGroup
	errs := make([]error, len(instances)*2)
	for i, instance := range instances {
		wg.Go(func() {
			_, errs[i*2] = instance.RegisterQueue(ctx, name, schema.QueueMeta{Retries: types.Ptr(uint64(5))}, noopTask)
		})
		wg.Go(func() {
			_, errs[i*2+1] = instance.RegisterTicker(ctx, name, schema.TickerMeta{Interval: types.Ptr(time.Hour)}, noopTask)
		})
	}
	wg.Wait()
	for _, err := range errs {
		assert.NoError(t, err)
	}

	// And there's one of each
	queue, err := manager.GetQueue(ctx, name)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), *queue.Retries)
	ticker, err := manager.GetTicker(ctx, name)
	require.NoError(t, err)
	assert.Equal(t, time.Hour, *ticker.Interval)
}
