package manager_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	test "github.com/mutablelogic/go-pg/pgqueue/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func noopTask(context.Context, json.RawMessage) (any, error) {
	return nil, nil
}

func TestCreateQueueDefaults(t *testing.T) {
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	queue, err := mgr.RegisterQueue(ctx, "queue_create_defaults", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)
	require.NotNil(t, queue)
	assert.Equal(t, "queue_create_defaults", queue.Queue)
	if assert.NotNil(t, queue.TTL) {
		assert.Equal(t, time.Hour, *queue.TTL)
	}
	if assert.NotNil(t, queue.Retries) {
		assert.EqualValues(t, 3, *queue.Retries)
	}
	if assert.NotNil(t, queue.RetryDelay) {
		assert.Equal(t, 2*time.Minute, *queue.RetryDelay)
	}
	if assert.NotNil(t, queue.Concurrency) {
		assert.EqualValues(t, 0, *queue.Concurrency)
	}

	_, _ = mgr.DeleteQueue(ctx, queue.Queue)
}

func TestCreateQueueWithPatch(t *testing.T) {
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	ttl := 3 * time.Hour
	retries := uint64(7)
	retryDelay := 5 * time.Minute
	concurrency := uint64(2)

	queue, err := mgr.RegisterQueue(ctx, "queue_create_patch", schema.QueueMeta{
		TTL:         &ttl,
		Retries:     &retries,
		RetryDelay:  &retryDelay,
		Concurrency: &concurrency,
	}, noopTask)
	require.NoError(t, err)
	require.NotNil(t, queue)
	assert.Equal(t, "queue_create_patch", queue.Queue)
	if assert.NotNil(t, queue.TTL) {
		assert.Equal(t, ttl, *queue.TTL)
	}
	if assert.NotNil(t, queue.Retries) {
		assert.Equal(t, retries, *queue.Retries)
	}
	if assert.NotNil(t, queue.RetryDelay) {
		assert.Equal(t, retryDelay, *queue.RetryDelay)
	}
	if assert.NotNil(t, queue.Concurrency) {
		assert.Equal(t, concurrency, *queue.Concurrency)
	}

	_, _ = mgr.DeleteQueue(ctx, queue.Queue)
}

func TestListQueues(t *testing.T) {
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	queue1, err := mgr.RegisterQueue(ctx, "queue_list_one", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)
	queue2, err := mgr.RegisterQueue(ctx, "queue_list_two", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)
	defer func() {
		_, _ = mgr.DeleteQueue(ctx, queue1.Queue)
		_, _ = mgr.DeleteQueue(ctx, queue2.Queue)
	}()

	limit := uint64(10)
	result, err := mgr.ListQueues(ctx, schema.QueueListRequest{OffsetLimit: pg.OffsetLimit{Limit: &limit}})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.GreaterOrEqual(t, result.Count, uint64(2))
	assert.GreaterOrEqual(t, len(result.Body), 2)
	if assert.NotNil(t, result.Limit) {
		assert.Equal(t, min(limit, result.Count), *result.Limit)
	}

	names := make([]string, 0, len(result.Body))
	for _, queue := range result.Body {
		names = append(names, queue.Queue)
	}
	assert.Contains(t, names, queue1.Queue)
	assert.Contains(t, names, queue2.Queue)
}

func TestGetUpdateDeleteQueue(t *testing.T) {
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	queue, err := mgr.RegisterQueue(ctx, "queue_get_update_delete", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)

	fetched, err := mgr.GetQueue(ctx, queue.Queue)
	require.NoError(t, err)
	require.NotNil(t, fetched)
	assert.Equal(t, queue.Queue, fetched.Queue)

	retries := uint64(9)
	updated, err := mgr.UpdateQueue(ctx, queue.Queue, schema.QueueMeta{Retries: &retries})
	require.NoError(t, err)
	require.NotNil(t, updated)
	if assert.NotNil(t, updated.Retries) {
		assert.Equal(t, retries, *updated.Retries)
	}

	deleted, err := mgr.DeleteQueue(ctx, queue.Queue)
	require.NoError(t, err)
	require.NotNil(t, deleted)
	assert.Equal(t, queue.Queue, deleted.Queue)

	_, err = mgr.GetQueue(ctx, queue.Queue)
	require.Error(t, err)
	assert.True(t, errors.Is(err, pg.ErrNotFound))
}

func TestZeroRetryQueueTasksStartFailed(t *testing.T) {
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	beforeSeq, err := mgr.GetPartitionSeq(ctx)
	require.NoError(t, err)

	nextID := beforeSeq + 1
	partitions, err := mgr.ListPartitions(ctx)
	require.NoError(t, err)

	createdPartition := ""
	if !partitionContains(partitions, nextID) {
		meta := schema.PartitionMeta{
			Partition: "task_partition_zero_retries_failed",
			Start:     1,
			End:       1000000,
		}
		require.NoError(t, mgr.DeletePartition(ctx, meta.Partition))
		require.NoError(t, mgr.CreatePartition(ctx, meta))
		createdPartition = meta.Partition
	}

	retries := uint64(0)
	queue, err := mgr.RegisterQueue(ctx, "queue_zero_retries_failed", schema.QueueMeta{Retries: &retries}, noopTask)
	require.NoError(t, err)
	defer func() {
		_, _ = mgr.DeleteQueue(ctx, queue.Queue)
		if createdPartition != "" {
			_ = mgr.DeletePartition(ctx, createdPartition)
		}
	}()

	var taskID schema.TaskId
	require.NoError(t, mgr.With("id", queue.Queue).Insert(ctx, &taskID, schema.TaskMeta{Payload: json.RawMessage(`{"kind":"zero-retry"}`)}))

	next, err := mgr.NextTask(ctx, "worker-zero-retries", queue.Queue)
	require.NoError(t, err)
	assert.Nil(t, next)

	var list schema.TaskList
	err = mgr.With("id", queue.Queue).List(ctx, &list, schema.TaskListRequest{Status: "failed"})
	require.NoError(t, err)
	assert.NotZero(t, list.Count)

	ids := make([]uint64, 0, len(list.Body))
	for _, item := range list.Body {
		ids = append(ids, item.Id)
	}
	assert.Contains(t, ids, uint64(taskID))
}

func TestNextTaskRespectsQueueConcurrency(t *testing.T) {
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	beforeSeq, err := mgr.GetPartitionSeq(ctx)
	require.NoError(t, err)

	nextID := beforeSeq + 1
	partitions, err := mgr.ListPartitions(ctx)
	require.NoError(t, err)

	createdPartition := ""
	if !partitionContains(partitions, nextID) {
		meta := schema.PartitionMeta{
			Partition: "task_partition_queue_concurrency",
			Start:     1,
			End:       1000000,
		}
		require.NoError(t, mgr.DeletePartition(ctx, meta.Partition))
		require.NoError(t, mgr.CreatePartition(ctx, meta))
		createdPartition = meta.Partition
	}

	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
	})
	started := make(chan struct{}, 2)

	concurrency := uint64(1)
	queue, err := mgr.RegisterQueue(ctx, "queue_concurrency_limit", schema.QueueMeta{Concurrency: &concurrency}, func(context.Context, json.RawMessage) (any, error) {
		started <- struct{}{}
		<-release
		return nil, nil
	})
	require.NoError(t, err)
	defer func() {
		_, _ = mgr.DeleteQueue(ctx, queue.Queue)
		if createdPartition != "" {
			_ = mgr.DeletePartition(ctx, createdPartition)
		}
	}()

	first, err := mgr.CreateTask(ctx, queue.Queue, schema.TaskMeta{Payload: json.RawMessage(`{"task":1}`)})
	require.NoError(t, err)
	_, err = mgr.CreateTask(ctx, queue.Queue, schema.TaskMeta{Payload: json.RawMessage(`{"task":2}`)})
	require.NoError(t, err)

	assert.NotZero(t, first.Id)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first queued task to start")
	}

	select {
	case <-started:
		t.Fatal("second queued task started despite concurrency limit")
	case <-time.After(200 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(release) })
}

func TestNextTaskWithZeroConcurrencyKeepsUnlimitedBehavior(t *testing.T) {
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	beforeSeq, err := mgr.GetPartitionSeq(ctx)
	require.NoError(t, err)

	nextID := beforeSeq + 1
	partitions, err := mgr.ListPartitions(ctx)
	require.NoError(t, err)

	createdPartition := ""
	if !partitionContains(partitions, nextID) {
		meta := schema.PartitionMeta{
			Partition: "task_partition_queue_concurrency_zero",
			Start:     1,
			End:       1000000,
		}
		require.NoError(t, mgr.DeletePartition(ctx, meta.Partition))
		require.NoError(t, mgr.CreatePartition(ctx, meta))
		createdPartition = meta.Partition
	}

	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
	})
	started := make(chan struct{}, 2)

	concurrency := uint64(0)
	queue, err := mgr.RegisterQueue(ctx, "queue_concurrency_unlimited", schema.QueueMeta{Concurrency: &concurrency}, func(context.Context, json.RawMessage) (any, error) {
		started <- struct{}{}
		<-release
		return nil, nil
	})
	require.NoError(t, err)
	defer func() {
		_, _ = mgr.DeleteQueue(ctx, queue.Queue)
		if createdPartition != "" {
			_ = mgr.DeletePartition(ctx, createdPartition)
		}
	}()

	first, err := mgr.CreateTask(ctx, queue.Queue, schema.TaskMeta{Payload: json.RawMessage(`{"task":1}`)})
	require.NoError(t, err)
	second, err := mgr.CreateTask(ctx, queue.Queue, schema.TaskMeta{Payload: json.RawMessage(`{"task":2}`)})
	require.NoError(t, err)

	assert.NotZero(t, first.Id)
	assert.NotZero(t, second.Id)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first queued task to start")
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second queued task to start")
	}

	releaseOnce.Do(func() { close(release) })
}

func TestNextTaskReclaimsExpiredRunningTask(t *testing.T) {
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	beforeSeq, err := mgr.GetPartitionSeq(ctx)
	require.NoError(t, err)

	nextID := beforeSeq + 1
	partitions, err := mgr.ListPartitions(ctx)
	require.NoError(t, err)

	createdPartition := ""
	if !partitionContains(partitions, nextID) {
		meta := schema.PartitionMeta{
			Partition: "task_partition_queue_reclaim_expired",
			Start:     1,
			End:       1000000,
		}
		require.NoError(t, mgr.DeletePartition(ctx, meta.Partition))
		require.NoError(t, mgr.CreatePartition(ctx, meta))
		createdPartition = meta.Partition
	}

	ttl := 100 * time.Millisecond
	concurrency := uint64(1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
	})
	started := make(chan struct{}, 2)
	var calls atomic.Uint32

	queue, err := mgr.RegisterQueue(ctx, "queue_reclaim_expired", schema.QueueMeta{
		TTL:         &ttl,
		Concurrency: &concurrency,
	}, func(context.Context, json.RawMessage) (any, error) {
		started <- struct{}{}
		if calls.Add(1) == 1 {
			<-release
		}
		return nil, nil
	})
	require.NoError(t, err)
	defer func() {
		releaseOnce.Do(func() { close(release) })
		_, _ = mgr.DeleteQueue(ctx, queue.Queue)
		if createdPartition != "" {
			_ = mgr.DeletePartition(ctx, createdPartition)
		}
	}()

	_, err = mgr.CreateTask(ctx, queue.Queue, schema.TaskMeta{Payload: json.RawMessage(`{"task":1}`)})
	require.NoError(t, err)
	_, err = mgr.CreateTask(ctx, queue.Queue, schema.TaskMeta{Payload: json.RawMessage(`{"task":2}`)})
	require.NoError(t, err)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first queue task to start")
	}

	time.Sleep(ttl + 75*time.Millisecond)
	_, err = mgr.CreateTask(ctx, queue.Queue, schema.TaskMeta{Payload: json.RawMessage(`{"task":3}`)})
	require.NoError(t, err)

	// With the fix, the expired running task no longer blocks queue progress,
	// so another task attempt can start after TTL expiry.
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for queue progress after expired running task")
	}
}

func TestRunQueueTaskResultIsReleasedDone(t *testing.T) {
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	beforeSeq, err := mgr.GetPartitionSeq(ctx)
	require.NoError(t, err)

	nextID := beforeSeq + 1
	partitions, err := mgr.ListPartitions(ctx)
	require.NoError(t, err)

	createdPartition := ""
	if !partitionContains(partitions, nextID) {
		meta := schema.PartitionMeta{
			Partition: "task_partition_queue_release_done",
			Start:     1,
			End:       1000000,
		}
		require.NoError(t, mgr.DeletePartition(ctx, meta.Partition))
		require.NoError(t, mgr.CreatePartition(ctx, meta))
		createdPartition = meta.Partition
	}

	started := make(chan struct{}, 1)
	queue, err := mgr.RegisterQueue(ctx, "queue_release_done", schema.QueueMeta{}, func(context.Context, json.RawMessage) (any, error) {
		started <- struct{}{}
		return map[string]bool{"ok": true}, nil
	})
	require.NoError(t, err)
	defer func() {
		_, _ = mgr.DeleteQueue(ctx, queue.Queue)
		if createdPartition != "" {
			_ = mgr.DeletePartition(ctx, createdPartition)
		}
	}()

	task, err := mgr.CreateTask(ctx, queue.Queue, schema.TaskMeta{Payload: json.RawMessage(`{"task":"done"}`)})
	require.NoError(t, err)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queue task to start")
	}

	require.Eventually(t, func() bool {
		list, err := mgr.ListTasks(ctx, schema.TaskListRequest{Status: "done"})
		if err != nil {
			return false
		}
		for _, item := range list.Body {
			if item.Id == task.Id {
				return true
			}
		}
		return false
	}, 2*time.Second, 50*time.Millisecond)
}

func TestRunQueueTaskResultIsReleasedFailed(t *testing.T) {
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	beforeSeq, err := mgr.GetPartitionSeq(ctx)
	require.NoError(t, err)

	nextID := beforeSeq + 1
	partitions, err := mgr.ListPartitions(ctx)
	require.NoError(t, err)

	createdPartition := ""
	if !partitionContains(partitions, nextID) {
		meta := schema.PartitionMeta{
			Partition: "task_partition_queue_release_failed",
			Start:     1,
			End:       1000000,
		}
		require.NoError(t, mgr.DeletePartition(ctx, meta.Partition))
		require.NoError(t, mgr.CreatePartition(ctx, meta))
		createdPartition = meta.Partition
	}

	retries := uint64(1)
	started := make(chan struct{}, 1)
	queue, err := mgr.RegisterQueue(ctx, "queue_release_failed", schema.QueueMeta{Retries: &retries}, func(context.Context, json.RawMessage) (any, error) {
		started <- struct{}{}
		return nil, errors.New("boom")
	})
	require.NoError(t, err)
	defer func() {
		_, _ = mgr.DeleteQueue(ctx, queue.Queue)
		if createdPartition != "" {
			_ = mgr.DeletePartition(ctx, createdPartition)
		}
	}()

	task, err := mgr.CreateTask(ctx, queue.Queue, schema.TaskMeta{Payload: json.RawMessage(`{"task":"failed"}`)})
	require.NoError(t, err)

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for failing queue task to start")
	}

	require.Eventually(t, func() bool {
		list, err := mgr.ListTasks(ctx, schema.TaskListRequest{Status: "failed"})
		if err != nil {
			return false
		}
		for _, item := range list.Body {
			if item.Id == task.Id {
				assert.JSONEq(t, `"boom"`, string(item.Result))
				return true
			}
		}
		return false
	}, 2*time.Second, 50*time.Millisecond)
}
