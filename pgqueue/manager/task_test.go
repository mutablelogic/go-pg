package manager_test

import (
	"encoding/json"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	test "github.com/mutablelogic/go-pg/pgqueue/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestCreateTask(t *testing.T) {
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
			Partition: "task_partition_create_task",
			Start:     1,
			End:       1000000,
		}
		require.NoError(t, mgr.DeletePartition(ctx, meta.Partition))
		require.NoError(t, mgr.CreatePartition(ctx, meta))
		createdPartition = meta.Partition
	}

	queue, err := mgr.RegisterQueue(ctx, "task_create_queue", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)
	defer func() {
		_, _ = mgr.DeleteQueue(ctx, queue.Queue)
		if createdPartition != "" {
			_ = mgr.DeletePartition(ctx, createdPartition)
		}
	}()

	payload := json.RawMessage(`{"kind":"create-task"}`)
	task, err := mgr.CreateTask(ctx, queue.Queue, schema.TaskMeta{Payload: payload})
	require.NoError(t, err)
	require.NotNil(t, task)
	assert.Equal(t, queue.Queue, task.Queue)
	assert.JSONEq(t, string(payload), string(task.Payload))
	assert.NotZero(t, task.Id)
	assert.False(t, task.DiesAt.IsZero())
}
