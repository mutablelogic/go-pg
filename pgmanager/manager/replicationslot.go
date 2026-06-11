package manager

import (
	"context"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - REPLICATION SLOTS

// ListReplicationSlots returns a list of replication slots with their status.
// Includes lag information for connected replicas.
func (manager *Manager) ListReplicationSlots(ctx context.Context, req schema.ReplicationSlotListRequest) (_ *schema.ReplicationSlotList, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "ListReplicationSlots",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	var result schema.ReplicationSlotList
	if err := manager.conn.List(ctx, &result, &req); err != nil {
		return nil, err
	}

	// Set the offset and limit in the result to reflect the actual count of items returned
	// which may be less than the requested limit if there are not enough items.
	result.ReplicationSlotListRequest = req
	result.OffsetLimit.Clamp(result.Count)

	return &result, nil
}

// GetReplicationSlot retrieves a single replication slot by name.
// Returns an error if the name is empty or the slot is not found.
func (manager *Manager) GetReplicationSlot(ctx context.Context, name string) (_ *schema.ReplicationSlot, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "GetReplicationSlot",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	// Validate input
	if name == "" {
		return nil, pg.ErrBadParameter.With("name is empty")
	}

	// Get the slot
	var slot schema.ReplicationSlot
	if err := manager.conn.Get(ctx, &slot, schema.ReplicationSlotName(name)); err != nil {
		return nil, err
	}
	return &slot, nil
}

// CreateReplicationSlot creates a new replication slot with the specified metadata.
// Type must be "physical" or "logical". Logical slots require a plugin name.
func (manager *Manager) CreateReplicationSlot(ctx context.Context, meta schema.ReplicationSlotMeta) (_ *schema.ReplicationSlot, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "CreateReplicationSlot",
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	// Validate input
	if err := meta.Validate(); err != nil {
		return nil, err
	}

	// Create the slot
	if err := manager.conn.Insert(ctx, nil, meta); err != nil {
		return nil, err
	}

	// Get the created slot
	var slot schema.ReplicationSlot
	if err := manager.conn.Get(ctx, &slot, schema.ReplicationSlotName(meta.Name)); err != nil {
		return nil, err
	}

	return &slot, nil
}

// DeleteReplicationSlot drops a replication slot by name.
// Returns the slot metadata before deletion.
func (manager *Manager) DeleteReplicationSlot(ctx context.Context, name string) (_ *schema.ReplicationSlot, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "DeleteReplicationSlot",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	// Validate input
	if name == "" {
		return nil, pg.ErrBadParameter.With("name is empty")
	}

	// Get the slot before deletion
	var slot schema.ReplicationSlot
	if err := manager.conn.Get(ctx, &slot, schema.ReplicationSlotName(name)); err != nil {
		return nil, err
	}

	// Delete the slot
	if err := manager.conn.Delete(ctx, nil, schema.ReplicationSlotName(name)); err != nil {
		return nil, err
	}

	return &slot, nil
}
