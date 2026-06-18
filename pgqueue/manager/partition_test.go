package manager_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	// Packages
	manager "github.com/mutablelogic/go-pg/pgqueue/manager"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	test "github.com/mutablelogic/go-pg/pgqueue/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestCreateListDeletePartition(t *testing.T) {
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	meta := schema.PartitionMeta{
		Partition: "task_partition_crud",
		Start:     900000000,
		End:       900001000,
	}

	require.NoError(t, mgr.DeletePartition(ctx, meta.Partition))
	require.NoError(t, mgr.CreatePartition(ctx, meta))
	defer func() {
		_ = mgr.DeletePartition(ctx, meta.Partition)
	}()

	partitions, err := mgr.ListPartitions(ctx)
	require.NoError(t, err)

	partition, ok := findPartitionByName(partitions, meta.Partition)
	require.True(t, ok)
	assert.Equal(t, meta.Partition, partition.Partition)
	assert.Equal(t, meta.Start, partition.Start)
	assert.Equal(t, meta.End, partition.End)
	assert.EqualValues(t, 0, partition.Count)

	require.NoError(t, mgr.DeletePartition(ctx, meta.Partition))

	partitions, err = mgr.ListPartitions(ctx)
	require.NoError(t, err)
	_, ok = findPartitionByName(partitions, meta.Partition)
	assert.False(t, ok)
}

func TestGetPartitionSeq(t *testing.T) {
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
			Partition: "task_partition_seq",
			Start:     1,
			End:       1000000,
		}
		require.NoError(t, mgr.DeletePartition(ctx, meta.Partition))
		require.NoError(t, mgr.CreatePartition(ctx, meta))
		createdPartition = meta.Partition
	}

	queue, err := mgr.RegisterQueue(ctx, "partition_seq_queue", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)
	defer func() {
		_, _ = mgr.DeleteQueue(ctx, queue.Queue)
		if createdPartition != "" {
			_ = mgr.DeletePartition(ctx, createdPartition)
		}
	}()

	var taskID1, taskID2 schema.TaskId
	require.NoError(t, mgr.With("id", queue.Queue).Insert(ctx, &taskID1, schema.TaskMeta{Payload: json.RawMessage(`{"n":1}`)}))
	require.NoError(t, mgr.With("id", queue.Queue).Insert(ctx, &taskID2, schema.TaskMeta{Payload: json.RawMessage(`{"n":2}`)}))

	afterSeq, err := mgr.GetPartitionSeq(ctx)
	require.NoError(t, err)
	assert.Greater(t, afterSeq, beforeSeq)
	assert.Equal(t, afterSeq, uint64(taskID2))
	assert.Greater(t, uint64(taskID2), uint64(taskID1))

	partitions, err = mgr.ListPartitions(ctx)
	require.NoError(t, err)
	partition, ok := findPartitionContaining(partitions, uint64(taskID2))
	require.True(t, ok)
	assert.GreaterOrEqual(t, partition.Count, uint64(2))
}

func TestCreateNextPartitionThresholdAndAhead(t *testing.T) {
	shared, ctx := test.Begin(t)
	defer test.End(t)

	schemaName := fmt.Sprintf("pgq_partition_%d", time.Now().UnixNano())
	mgr, err := manager.New(ctx, shared.PoolConn,
		manager.WithSchema(schemaName),
		manager.WithPartitionSize(10),
		manager.WithPartitionThreshold(0.5),
		manager.WithPartitionAhead(2),
	)
	require.NoError(t, err)

	partitions, err := mgr.ListPartitions(ctx)
	require.NoError(t, err)
	require.Len(t, partitions, 2)

	queue, err := mgr.RegisterQueue(ctx, "partition_threshold_queue", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)
	defer func() {
		_, _ = mgr.DeleteQueue(ctx, queue.Queue)
	}()

	for i := 0; i < 11; i++ {
		var taskID schema.TaskId
		err := mgr.With("id", queue.Queue).Insert(ctx, &taskID, schema.TaskMeta{Payload: json.RawMessage(`{"n":1}`)})
		require.NoError(t, err)
	}

	created, err := mgr.CreateNextPartition(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, created)

	partitions, err = mgr.ListPartitions(ctx)
	require.NoError(t, err)
	assert.Len(t, partitions, 4)
}

func findPartitionByName(partitions []schema.Partition, name string) (schema.Partition, bool) {
	for _, partition := range partitions {
		if partition.Partition == name {
			return partition, true
		}
	}
	return schema.Partition{}, false
}

func findPartitionContaining(partitions []schema.Partition, id uint64) (schema.Partition, bool) {
	for _, partition := range partitions {
		if partitionContains([]schema.Partition{partition}, id) {
			return partition, true
		}
	}
	return schema.Partition{}, false
}

func partitionContains(partitions []schema.Partition, id uint64) bool {
	for _, partition := range partitions {
		if partition.Start <= id && id < partition.End {
			return true
		}
	}
	return false
}
