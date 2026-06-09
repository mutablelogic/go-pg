package manager

import (
	"context"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - REPLICATION SLOTS

// ListReplicationSlots returns a list of replication slots with their status.
// Includes lag information for connected replicas.
func (manager *Manager) ListReplicationSlots(ctx context.Context, req schema.ReplicationSlotListRequest) (*schema.ReplicationSlotList, error) {
	var list schema.ReplicationSlotList
	if err := manager.conn.List(ctx, &list, req); err != nil {
		return nil, err
	}
	return &list, nil
}

// GetReplicationSlot retrieves a single replication slot by name.
// Returns an error if the name is empty or the slot is not found.
func (manager *Manager) GetReplicationSlot(ctx context.Context, name string) (*schema.ReplicationSlot, error) {
	if name == "" {
		return nil, pg.ErrBadParameter.With("name is empty")
	}
	var slot schema.ReplicationSlot
	if err := manager.conn.Get(ctx, &slot, schema.ReplicationSlotName(name)); err != nil {
		return nil, err
	}
	return &slot, nil
}

// CreateReplicationSlot creates a new replication slot with the specified metadata.
// Type must be "physical" or "logical". Logical slots require a plugin name.
func (manager *Manager) CreateReplicationSlot(ctx context.Context, meta schema.ReplicationSlotMeta) (*schema.ReplicationSlot, error) {
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
func (manager *Manager) DeleteReplicationSlot(ctx context.Context, name string) (*schema.ReplicationSlot, error) {
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
