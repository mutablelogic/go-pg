package queue_test

import (
	"context"
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

	mgr, err := queue.New(ctx, conn, "test_register")
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

	mgr, err := queue.New(ctx, conn, "test_list")
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

	mgr, err := queue.New(ctx, conn, "test_get")
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

	mgr, err := queue.New(ctx, conn, "test_delete")
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

	mgr, err := queue.New(ctx, conn, "test_update")
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

////////////////////////////////////////////////////////////////////////////////
// HELPER FUNCTIONS

func ptr[T any](v T) *T {
	return &v
}
