package pgqueue_test

import (
	"testing"

	// Packages
	pgqueue "github.com/mutablelogic/go-pg/pgqueue"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	test "github.com/mutablelogic/go-pg/pgqueue/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestPartition(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)

	t.Run("New", func(t *testing.T) {
		// A new manager has one partition, from the start of the sequence
		manager := test.IsolatedManager(t, pgqueue.WithPartitionSize(10))
		partitions, err := manager.ListPartitions(ctx)
		require.NoError(t, err)
		require.Len(t, partitions, 1)
		assert.Equal(t, schema.PartitionMeta{Partition: "task_00000001_00000011", Start: 1, End: 11}, partitions[0].PartitionMeta)
		assert.Zero(t, partitions[0].Count)

		seq, err := manager.GetPartitionSeq(ctx)
		require.NoError(t, err)
		assert.EqualValues(t, 1, seq)
	})

	t.Run("CreateListDelete", func(t *testing.T) {
		manager := test.IsolatedManager(t)
		meta := schema.PartitionMeta{Partition: "task_partition_crud", Start: 900000000, End: 900001000}
		require.NoError(t, manager.CreatePartition(ctx, meta))

		partitions, err := manager.ListPartitions(ctx)
		require.NoError(t, err)
		partition, ok := findPartition(partitions, meta.Partition)
		require.True(t, ok)
		assert.Equal(t, meta, partition.PartitionMeta)
		assert.Zero(t, partition.Count)

		require.NoError(t, manager.DeletePartition(ctx, meta.Partition))
		partitions, err = manager.ListPartitions(ctx)
		require.NoError(t, err)
		_, ok = findPartition(partitions, meta.Partition)
		assert.False(t, ok)

		// Deleting a partition which doesn't exist isn't an error
		assert.NoError(t, manager.DeletePartition(ctx, meta.Partition))
	})

	t.Run("Invalid", func(t *testing.T) {
		manager := test.IsolatedManager(t)
		assert.Error(t, manager.CreatePartition(ctx, schema.PartitionMeta{Partition: "task partition", Start: 1, End: 2}))
		assert.Error(t, manager.CreatePartition(ctx, schema.PartitionMeta{Partition: "task_backwards", Start: 2, End: 1}))
		assert.Error(t, manager.DeletePartition(ctx, "task; DROP TABLE task"))
	})

	t.Run("CreateNext", func(t *testing.T) {
		// A partition is only created when the sequence crosses the
		// threshold of the highest partition
		manager := test.IsolatedManager(t, pgqueue.WithPartitionSize(10), pgqueue.WithPartitionAhead(2))
		created, err := manager.CreateNextPartition(ctx)
		require.NoError(t, err)
		assert.Empty(t, created)

		// Without partitions, the partitions ahead are created from the
		// start of the sequence
		partitions, err := manager.ListPartitions(ctx)
		require.NoError(t, err)
		require.Len(t, partitions, 2)
		for _, partition := range partitions {
			require.NoError(t, manager.DeletePartition(ctx, partition.Partition))
		}
		created, err = manager.CreateNextPartition(ctx)
		require.NoError(t, err)
		assert.Equal(t, "task_00000001_00000011 (+1 more)", created)

		partitions, err = manager.ListPartitions(ctx)
		require.NoError(t, err)
		require.Len(t, partitions, 2)
		assert.Equal(t, "task_00000011_00000021", partitions[1].Partition)
	})

	t.Run("DropDrained", func(t *testing.T) {
		manager := test.IsolatedManager(t, pgqueue.WithPartitionSize(10))

		// The only partition is kept
		dropped, err := manager.DropDrainedPartition(ctx)
		require.NoError(t, err)
		assert.Empty(t, dropped)

		// An empty partition which the sequence has passed is dropped
		require.NoError(t, manager.CreatePartition(ctx, schema.PartitionMeta{Partition: "task_old", Start: 0, End: 1}))
		dropped, err = manager.DropDrainedPartition(ctx)
		require.NoError(t, err)
		assert.Equal(t, "task_old", dropped)

		// An empty partition which the sequence hasn't passed is kept
		require.NoError(t, manager.CreatePartition(ctx, schema.PartitionMeta{Partition: "task_next", Start: 11, End: 21}))
		dropped, err = manager.DropDrainedPartition(ctx)
		require.NoError(t, err)
		assert.Empty(t, dropped)
	})
}

// findPartition returns the partition with the name
func findPartition(partitions []schema.Partition, name string) (schema.Partition, bool) {
	for _, partition := range partitions {
		if partition.Partition == name {
			return partition, true
		}
	}
	return schema.Partition{}, false
}
