package manager

import (
	"context"
	"fmt"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) CreateNextPartition(ctx context.Context) (result string, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "CreateNextPartition")
	defer func() { endSpan(err) }()

	// Get current sequence value
	seq, err := manager.GetPartitionSeq(ctx)
	if err != nil {
		return "", err
	}

	// Get existing partitions to find the highest upper bound
	partitions, err := manager.ListPartitions(ctx)
	if err != nil {
		return "", err
	}

	// Find the highest upper bound
	var maxEnd uint64
	for _, p := range partitions {
		if p.End > maxEnd {
			maxEnd = p.End
		}
	}

	// Create partition(s) if no partitions exist, or sequence has crossed threshold.
	if maxEnd == 0 || seq >= uint64(float64(maxEnd)*manager.partitionThreshold) {
		count := manager.partitionAhead
		if count == 0 {
			count = 1
		}

		created := make([]string, 0, count)
		for i := uint64(0); i < count; i++ {
			start := maxEnd
			if start == 0 {
				start = 1
			}
			end := start + manager.partitionSize
			name := fmt.Sprintf("task_%08d_%08d", start, end)
			if err := manager.CreatePartition(ctx, schema.PartitionMeta{
				Partition: name,
				Start:     start,
				End:       end,
			}); err != nil {
				return "", err
			}

			created = append(created, name)
			maxEnd = end
		}

		// Reset the connection to refresh partition metadata.
		manager.PoolConn.Reset()

		if len(created) == 1 {
			return created[0], nil
		}

		return fmt.Sprintf("%s (+%d more)", created[0], len(created)-1), nil
	}

	return "", nil
}

func (manager *Manager) DropDrainedPartition(ctx context.Context) (result string, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "DropDrainedPartition")
	defer func() { endSpan(err) }()

	// Get all the current partitions, and drop the oldest drained partition if it exists.
	partitions, err := manager.ListPartitions(ctx)
	if err != nil {
		return "", err
	}
	// Always keep at least one partition
	if len(partitions) <= 1 {
		return "", nil
	}

	// Determine the oldest partition by lower bound, independent of name order.
	oldest := partitions[0]
	for _, partition := range partitions[1:] {
		if partition.Start < oldest.Start || (partition.Start == oldest.Start && (partition.End < oldest.End || (partition.End == oldest.End && partition.Partition < oldest.Partition))) {
			oldest = partition
		}
	}
	if oldest.Count > 0 {
		return "", nil
	}

	// Don't drop a partition if the sequence hasn't advanced past its end — new
	// inserts could still land in this range.
	seq, err := manager.GetPartitionSeq(ctx)
	if err != nil {
		return "", err
	}
	if seq < oldest.End {
		return "", nil
	}
	if err := manager.DeletePartition(ctx, oldest.Partition); err != nil {
		return "", err
	} else {
		// Reset the connection
		manager.PoolConn.Reset()
	}
	return oldest.Partition, nil
}

func (manager *Manager) CreatePartition(ctx context.Context, meta schema.PartitionMeta) (err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "CreatePartition",
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	if err := manager.PoolConn.Insert(ctx, nil, meta); err != nil {
		return err
	}
	return nil

}

func (manager *Manager) GetPartitionSeq(ctx context.Context) (result uint64, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "GetPartitionSeq")
	defer func() { endSpan(err) }()

	var seq schema.PartitionSeq
	if err := manager.Get(ctx, &seq, schema.PartitionSeqRequest{}); err != nil {
		return 0, err
	}
	return uint64(seq), nil
}

func (manager *Manager) ListPartitions(ctx context.Context) (result []schema.Partition, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "ListPartitions")
	defer func() { endSpan(err) }()

	var resp schema.PartitionList
	if err := manager.List(ctx, &resp, schema.PartitionListRequest{}); err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (manager *Manager) DeletePartition(ctx context.Context, name string) (err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "DeletePartition",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	if err := manager.Delete(ctx, nil, schema.PartitionName(name)); err != nil {
		return err
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////
