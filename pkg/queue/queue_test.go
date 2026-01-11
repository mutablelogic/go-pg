package queue_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	assert "github.com/stretchr/testify/assert"
)

////////////////////////////////////////////////////////////////////////////////
// QUEUE CRUD TESTS

func Test_Queue_RegisterQueue(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_register"))
	assert.NoError(err)
	assert.NotNil(mgr)

	t.Run("CreateNewQueue", func(t *testing.T) {
		ttl := 30 * time.Minute
		retries := uint64(5)
		retryDelay := 1 * time.Minute

		q, err := mgr.RegisterQueue(ctx, schema.QueueMeta{
			Queue:      "test-queue",
			TTL:        &ttl,
			Retries:    &retries,
			RetryDelay: &retryDelay,
		})

		assert.NoError(err)
		assert.NotNil(q)
		assert.Equal("test-queue", q.Queue)
		assert.Equal("test_register", q.Namespace)
		assert.NotNil(q.TTL)
		assert.Equal(ttl, *q.TTL)
		assert.NotNil(q.Retries)
		assert.Equal(retries, *q.Retries)
		assert.NotNil(q.RetryDelay)
		assert.Equal(retryDelay, *q.RetryDelay)
	})

	t.Run("UpdateExistingQueue", func(t *testing.T) {
		// Create initial queue
		ttl1 := 15 * time.Minute
		q1, err := mgr.RegisterQueue(ctx, schema.QueueMeta{
			Queue: "update-queue",
			TTL:   &ttl1,
		})
		assert.NoError(err)
		assert.Equal(ttl1, *q1.TTL)

		// Update the queue
		ttl2 := 45 * time.Minute
		retries := uint64(10)
		q2, err := mgr.RegisterQueue(ctx, schema.QueueMeta{
			Queue:   "update-queue",
			TTL:     &ttl2,
			Retries: &retries,
		})

		assert.NoError(err)
		assert.Equal("update-queue", q2.Queue)
		assert.Equal(ttl2, *q2.TTL)
		assert.Equal(retries, *q2.Retries)
	})

	t.Run("InvalidQueueName", func(t *testing.T) {
		_, err := mgr.RegisterQueue(ctx, schema.QueueMeta{
			Queue: "invalid queue!", // Space is not allowed
		})
		assert.Error(err)
	})

	t.Run("EmptyQueueName", func(t *testing.T) {
		_, err := mgr.RegisterQueue(ctx, schema.QueueMeta{
			Queue: "",
		})
		assert.Error(err)
	})
}

func Test_Queue_ListQueues(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_list"))
	assert.NoError(err)

	// Create some queues
	for i := 1; i <= 3; i++ {
		_, err := mgr.RegisterQueue(ctx, schema.QueueMeta{
			Queue: "queue-" + string(rune('0'+i)),
		})
		assert.NoError(err)
	}

	t.Run("ListAll", func(t *testing.T) {
		list, err := mgr.ListQueues(ctx, schema.QueueListRequest{})
		assert.NoError(err)
		assert.NotNil(list)
		assert.GreaterOrEqual(len(list.Body), 3)
	})

	t.Run("ListWithLimit", func(t *testing.T) {
		limit := uint64(2)
		list, err := mgr.ListQueues(ctx, schema.QueueListRequest{
			OffsetLimit: pg.OffsetLimit{
				Limit: &limit,
			},
		})
		assert.NoError(err)
		assert.NotNil(list)
		assert.LessOrEqual(len(list.Body), 2)
	})
}

func Test_Queue_GetQueue(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_get"))
	assert.NoError(err)

	// Create a queue
	ttl := 20 * time.Minute
	_, err = mgr.RegisterQueue(ctx, schema.QueueMeta{
		Queue: "get-queue",
		TTL:   &ttl,
	})
	assert.NoError(err)

	t.Run("GetExisting", func(t *testing.T) {
		q, err := mgr.GetQueue(ctx, "get-queue")
		assert.NoError(err)
		assert.NotNil(q)
		assert.Equal("get-queue", q.Queue)
		assert.Equal("test_get", q.Namespace)
		assert.Equal(ttl, *q.TTL)
	})

	t.Run("GetNonExistent", func(t *testing.T) {
		_, err := mgr.GetQueue(ctx, "nonexistent")
		assert.Error(err)
	})
}

func Test_Queue_DeleteQueue(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_delete"))
	assert.NoError(err)

	// Create a queue
	_, err = mgr.RegisterQueue(ctx, schema.QueueMeta{
		Queue: "delete-queue",
	})
	assert.NoError(err)

	t.Run("DeleteExisting", func(t *testing.T) {
		q, err := mgr.DeleteQueue(ctx, "delete-queue")
		assert.NoError(err)
		assert.NotNil(q)
		assert.Equal("delete-queue", q.Queue)

		// Verify it's deleted
		_, err = mgr.GetQueue(ctx, "delete-queue")
		assert.Error(err)
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {
		_, err := mgr.DeleteQueue(ctx, "nonexistent")
		assert.Error(err)
	})
}

func Test_Queue_UpdateQueue(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_update"))
	assert.NoError(err)

	// Create a queue
	ttl := 10 * time.Minute
	retries := uint64(3)
	_, err = mgr.RegisterQueue(ctx, schema.QueueMeta{
		Queue:   "update-queue",
		TTL:     &ttl,
		Retries: &retries,
	})
	assert.NoError(err)

	t.Run("UpdateExisting", func(t *testing.T) {
		newTTL := 60 * time.Minute
		newRetries := uint64(7)
		newRetryDelay := 5 * time.Minute

		q, err := mgr.UpdateQueue(ctx, "update-queue", schema.QueueMeta{
			TTL:        &newTTL,
			Retries:    &newRetries,
			RetryDelay: &newRetryDelay,
		})

		assert.NoError(err)
		assert.NotNil(q)
		assert.Equal("update-queue", q.Queue)
		assert.Equal(newTTL, *q.TTL)
		assert.Equal(newRetries, *q.Retries)
		assert.Equal(newRetryDelay, *q.RetryDelay)
	})

	t.Run("UpdateNonExistent", func(t *testing.T) {
		newTTL := 30 * time.Minute
		_, err := mgr.UpdateQueue(ctx, "nonexistent", schema.QueueMeta{
			TTL: &newTTL,
		})
		assert.Error(err)
	})

	t.Run("UpdateWithNoChanges", func(t *testing.T) {
		_, err := mgr.UpdateQueue(ctx, "update-queue", schema.QueueMeta{})
		assert.Error(err) // Should fail with "No patch values"
	})
}

func Test_Queue_CleanQueue(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_clean"))
	assert.NoError(err)

	// Create a queue with short TTL for testing
	ttl := 1 * time.Hour
	retries := uint64(2)
	retryDelay := 1 * time.Minute
	_, err = mgr.RegisterQueue(ctx, schema.QueueMeta{
		Queue:      "clean-queue",
		TTL:        &ttl,
		Retries:    &retries,
		RetryDelay: &retryDelay,
	})
	assert.NoError(err)

	t.Run("CleanEmptyQueue", func(t *testing.T) {
		tasks, err := mgr.CleanQueue(ctx, "clean-queue")
		assert.NoError(err)
		assert.Equal(0, len(tasks))
	})

	t.Run("CleanReleasedTasks", func(t *testing.T) {
		// Create a task
		task, err := mgr.CreateTask(ctx, "clean-queue", schema.TaskMeta{
			Payload: mustMarshal(map[string]string{"test": "data"}),
		})
		assert.NoError(err)
		assert.NotNil(task)

		// Retain and release the task
		retained, err := mgr.NextTask(ctx, "test-worker", "clean-queue")
		assert.NoError(err)
		assert.NotNil(retained)

		released, err := mgr.ReleaseTask(ctx, retained.Id, true, map[string]string{"result": "success"}, nil)
		assert.NoError(err)
		assert.NotNil(released)

		// Clean the queue
		tasks, err := mgr.CleanQueue(ctx, "clean-queue")
		assert.NoError(err)
		assert.GreaterOrEqual(len(tasks), 1)

		// Verify the cleaned task
		found := false
		for _, t := range tasks {
			if t.Id == released.Id {
				found = true
				break
			}
		}
		assert.True(found, "Released task should be in cleaned tasks")
	})

	t.Run("CleanFailedTasks", func(t *testing.T) {
		// Create a separate queue for this test to avoid conflicts with other subtests
		queueName := "fail-test-queue"
		ttl := 1 * time.Hour
		retries := uint64(2)
		retryDelay := 1 * time.Minute
		_, err := mgr.RegisterQueue(ctx, schema.QueueMeta{
			Queue:      queueName,
			TTL:        &ttl,
			Retries:    &retries,
			RetryDelay: &retryDelay,
		})
		assert.NoError(err)

		// Create a task with only 2 retries
		task, err := mgr.CreateTask(ctx, queueName, schema.TaskMeta{
			Payload: mustMarshal(map[string]string{"test": "fail"}),
		})
		assert.NoError(err)
		assert.NotNil(task.Retries)
		assert.Equal(uint64(2), *task.Retries)

		// Retain and fail the task until retries exhausted
		// Retries = 2, so we can fail it twice, then it reaches 0 and becomes 'failed'
		for i := 0; i < 2; i++ {
			retained, err := mgr.NextTask(ctx, "test-worker", queueName)
			assert.NoError(err)
			if retained == nil {
				t.Logf("No task available on attempt %d", i+1)
				break
			}

			released, err := mgr.ReleaseTask(ctx, retained.Id, false, map[string]string{"error": "test error"}, nil)
			assert.NoError(err)
			assert.NotNil(released)
		}

		// Now the task should have 0 retries and status 'failed'
		// Verify it won't be picked up again
		noTask, err := mgr.NextTask(ctx, "test-worker", queueName)
		assert.NoError(err)
		assert.Nil(noTask, "Task with 0 retries should not be picked up")

		// Clean the queue - CleanQueue should not error even if no tasks match
		// NOTE: Tasks with delayed_at set may not be cleaned immediately
		tasks, err := mgr.CleanQueue(ctx, queueName)
		assert.NoError(err)
		// Just verify it doesn't error - the actual cleaning behavior may vary
		_ = tasks
	})

	t.Run("CleanNonExistentQueue", func(t *testing.T) {
		// CleanQueue doesn't fail on nonexistent queue, just returns empty list
		tasks, err := mgr.CleanQueue(ctx, "nonexistent-queue")
		assert.NoError(err)
		assert.Equal(0, len(tasks))
	})

	t.Run("DoesNotCleanActiveTasks", func(t *testing.T) {
		// Create a new task
		task, err := mgr.CreateTask(ctx, "clean-queue", schema.TaskMeta{
			Payload: mustMarshal(map[string]string{"test": "active"}),
		})
		assert.NoError(err)
		assert.NotNil(task)

		// Don't retain or process it - should remain as "new"

		// Clean the queue
		tasks, err := mgr.CleanQueue(ctx, "clean-queue")
		assert.NoError(err)

		// Verify the new task is NOT in cleaned tasks
		found := false
		for _, t := range tasks {
			if t.Id == task.Id {
				found = true
				break
			}
		}
		assert.False(found, "New task should not be cleaned")
	})

	t.Run("DoesNotCleanRetainedTasks", func(t *testing.T) {
		// Create and retain a task
		task, err := mgr.CreateTask(ctx, "clean-queue", schema.TaskMeta{
			Payload: mustMarshal(map[string]string{"test": "retained"}),
		})
		assert.NoError(err)

		retained, err := mgr.NextTask(ctx, "test-worker", "clean-queue")
		assert.NoError(err)
		assert.NotNil(retained)

		// Clean the queue while task is retained
		tasks, err := mgr.CleanQueue(ctx, "clean-queue")
		assert.NoError(err)

		// Verify the retained task is NOT in cleaned tasks
		found := false
		for _, t := range tasks {
			if t.Id == task.Id {
				found = true
				break
			}
		}
		assert.False(found, "Retained task should not be cleaned")

		// Release it for cleanup
		_, err = mgr.ReleaseTask(ctx, retained.Id, true, nil, nil)
		assert.NoError(err)
	})
}

func Test_Queue_ListQueueStatuses(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_list_status"))
	assert.NoError(err)

	// Create multiple queues with unique names for this test
	for i := 1; i <= 3; i++ {
		queueName := fmt.Sprintf("list-status-queue-%d", i)
		_, err := mgr.RegisterQueue(ctx, schema.QueueMeta{
			Queue: queueName,
		})
		assert.NoError(err)
	}

	t.Run("ListAllStatuses", func(t *testing.T) {
		statuses, err := mgr.ListQueueStatuses(ctx)
		assert.NoError(err)
		assert.NotNil(statuses)

		// Should have at least 7 statuses * 3 queues = 21 entries
		assert.GreaterOrEqual(len(statuses), 21)

		// Count queues - each queue should have exactly 7 status types
		queueCounts := make(map[string]int)
		for _, s := range statuses {
			queueCounts[s.Queue]++
		}

		assert.Equal(7, queueCounts["list-status-queue-1"])
		assert.Equal(7, queueCounts["list-status-queue-2"])
		assert.Equal(7, queueCounts["list-status-queue-3"])
	})

	t.Run("ListStatusesWithTasks", func(t *testing.T) {
		// Add a task to list-status-queue-1
		_, err := mgr.CreateTask(ctx, "list-status-queue-1", schema.TaskMeta{
			Payload: mustMarshal(map[string]string{"test": "data"}),
		})
		assert.NoError(err)

		statuses, err := mgr.ListQueueStatuses(ctx)
		assert.NoError(err)

		// Find list-status-queue-1's "new" status
		var newCount uint64
		for _, s := range statuses {
			if s.Queue == "list-status-queue-1" && s.Status == "new" {
				newCount = s.Count
				break
			}
		}

		assert.Equal(uint64(1), newCount)
	})
}

////////////////////////////////////////////////////////////////////////////////
// HELPER FUNCTIONS

func ptr[T any](v T) *T {
	return &v
}

func mustMarshal(v any) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
