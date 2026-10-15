package pgqueue

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

func (self *Manager) CreateNextPartition(ctx context.Context) (result string, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.CreateNextPartition")
	defer func() { endSpan(err) }()

	// Get current sequence value
	seq, err := self.GetPartitionSeq(ctx)
	if err != nil {
		return "", err
	}

	// Get existing partitions to find the highest upper bound
	partitions, err := self.ListPartitions(ctx)
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
	if maxEnd == 0 || seq >= uint64(float64(maxEnd)*self.partitionThreshold) {
		count := self.partitionAhead
		if count == 0 {
			count = 1
		}

		created := make([]string, 0, count)
		for i := uint64(0); i < count; i++ {
			start := maxEnd
			if start == 0 {
				start = 1
			}
			end := start + self.partitionSize
			name := fmt.Sprintf("task_%08d_%08d", start, end)
			if err := self.CreatePartition(ctx, schema.PartitionMeta{
				Partition: name,
				Start:     start,
				End:       end,
			}); err != nil {
				return "", err
			}

			created = append(created, name)
			maxEnd = end
		}

		if len(created) == 1 {
			return created[0], nil
		}

		return fmt.Sprintf("%s (+%d more)", created[0], len(created)-1), nil
	}

	return "", nil
}

func (self *Manager) DropDrainedPartition(ctx context.Context) (result string, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.DropDrainedPartition")
	defer func() { endSpan(err) }()

	// Get all the current partitions, and drop the oldest drained partition if it exists.
	partitions, err := self.ListPartitions(ctx)
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
	seq, err := self.GetPartitionSeq(ctx)
	if err != nil {
		return "", err
	}
	if seq < oldest.End {
		return "", nil
	}
	if err := self.DeletePartition(ctx, oldest.Partition); err != nil {
		return "", err
	}
	return oldest.Partition, nil
}

func (self *Manager) CreatePartition(ctx context.Context, meta schema.PartitionMeta) (err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.CreatePartition",
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	if err := self.PoolConn.Insert(ctx, nil, meta); err != nil {
		return err
	}
	return nil

}

func (self *Manager) GetPartitionSeq(ctx context.Context) (result uint64, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.GetPartitionSeq")
	defer func() { endSpan(err) }()

	var seq schema.PartitionSeq
	if err := self.Get(ctx, &seq, schema.PartitionSeqRequest{}); err != nil {
		return 0, err
	}
	return uint64(seq), nil
}

func (self *Manager) ListPartitions(ctx context.Context) (result []schema.Partition, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.ListPartitions")
	defer func() { endSpan(err) }()

	var resp schema.PartitionList
	if err := self.List(ctx, &resp, schema.PartitionListRequest{}); err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (self *Manager) DeletePartition(ctx context.Context, name string) (err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.DeletePartition",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	if err := self.Delete(ctx, nil, schema.PartitionName(name)); err != nil {
		return err
	}
	return nil
}
