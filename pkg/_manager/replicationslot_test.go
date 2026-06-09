package manager_test

import (
	"context"
	"strings"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pkg/manager"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	assert "github.com/stretchr/testify/assert"
)

////////////////////////////////////////////////////////////////////////////////
// LIST REPLICATION SLOTS TESTS

func Test_Manager_ListReplicationSlots(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("ListAll", func(t *testing.T) {
		slots, err := mgr.ListReplicationSlots(context.TODO(), schema.ReplicationSlotListRequest{})
		assert.NoError(err)
		assert.NotNil(slots)
		// Count should match body length
		assert.Equal(len(slots.Body), int(slots.Count))
	})

	t.Run("ListWithPagination", func(t *testing.T) {
		limit := uint64(1)
		slots, err := mgr.ListReplicationSlots(context.TODO(), schema.ReplicationSlotListRequest{
			OffsetLimit: pg.OffsetLimit{Limit: &limit},
		})
		assert.NoError(err)
		assert.NotNil(slots)
		assert.LessOrEqual(len(slots.Body), 1)
	})
}

////////////////////////////////////////////////////////////////////////////////
// GET REPLICATION SLOT TESTS

func Test_Manager_GetReplicationSlot(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("GetNonExistent", func(t *testing.T) {
		_, err := mgr.GetReplicationSlot(context.TODO(), "non_existing_slot_xyz")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("GetEmptyName", func(t *testing.T) {
		_, err := mgr.GetReplicationSlot(context.TODO(), "")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})
}

////////////////////////////////////////////////////////////////////////////////
// CREATE REPLICATION SLOT TESTS

func Test_Manager_CreateReplicationSlot(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("CreatePhysical", func(t *testing.T) {
		slotName := "test_physical_slot"
		t.Cleanup(func() {
			mgr.DeleteReplicationSlot(context.TODO(), slotName)
		})

		slot, err := mgr.CreateReplicationSlot(context.TODO(), schema.ReplicationSlotMeta{
			Name: slotName,
			Type: "physical",
		})
		assert.NoError(err)
		assert.NotNil(slot)
		assert.Equal(slotName, slot.Name)
		assert.Equal("physical", slot.Type)
		assert.Equal("inactive", slot.Status)
	})

	t.Run("CreateLogical", func(t *testing.T) {
		slotName := "test_logical_slot"
		t.Cleanup(func() {
			mgr.DeleteReplicationSlot(context.TODO(), slotName)
		})

		slot, err := mgr.CreateReplicationSlot(context.TODO(), schema.ReplicationSlotMeta{
			Name:   slotName,
			Type:   "logical",
			Plugin: "pgoutput",
		})
		// Skip if wal_level is not sufficient for logical replication
		if err != nil && strings.Contains(err.Error(), "wal_level") {
			t.Skip("wal_level not set to logical")
		}
		assert.NoError(err)
		if slot == nil {
			t.FailNow()
		}
		assert.Equal(slotName, slot.Name)
		assert.Equal("logical", slot.Type)
		assert.Equal("pgoutput", slot.Plugin)
		assert.Equal("inactive", slot.Status)
	})

	t.Run("CreateTemporary", func(t *testing.T) {
		slotName := "test_temp_slot"
		// Temporary slots are deleted on disconnect, but cleanup anyway
		t.Cleanup(func() {
			mgr.DeleteReplicationSlot(context.TODO(), slotName)
		})

		slot, err := mgr.CreateReplicationSlot(context.TODO(), schema.ReplicationSlotMeta{
			Name:      slotName,
			Type:      "physical",
			Temporary: true,
		})
		assert.NoError(err)
		assert.NotNil(slot)
		assert.Equal(slotName, slot.Name)
		assert.True(slot.Temporary)
	})

	t.Run("CreateEmptyName", func(t *testing.T) {
		_, err := mgr.CreateReplicationSlot(context.TODO(), schema.ReplicationSlotMeta{
			Name: "",
			Type: "physical",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("CreateInvalidType", func(t *testing.T) {
		_, err := mgr.CreateReplicationSlot(context.TODO(), schema.ReplicationSlotMeta{
			Name: "test_invalid_type",
			Type: "invalid",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("CreateLogicalWithoutPlugin", func(t *testing.T) {
		_, err := mgr.CreateReplicationSlot(context.TODO(), schema.ReplicationSlotMeta{
			Name: "test_logical_no_plugin",
			Type: "logical",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("CreateReservedPrefix", func(t *testing.T) {
		_, err := mgr.CreateReplicationSlot(context.TODO(), schema.ReplicationSlotMeta{
			Name: "pg_reserved_slot",
			Type: "physical",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		slotName := "test_duplicate_slot"
		t.Cleanup(func() {
			mgr.DeleteReplicationSlot(context.TODO(), slotName)
		})

		_, err := mgr.CreateReplicationSlot(context.TODO(), schema.ReplicationSlotMeta{
			Name: slotName,
			Type: "physical",
		})
		assert.NoError(err)

		// Try to create again
		_, err = mgr.CreateReplicationSlot(context.TODO(), schema.ReplicationSlotMeta{
			Name: slotName,
			Type: "physical",
		})
		assert.Error(err)
	})
}

////////////////////////////////////////////////////////////////////////////////
// DELETE REPLICATION SLOT TESTS

func Test_Manager_DeleteReplicationSlot(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("DeleteExisting", func(t *testing.T) {
		slotName := "test_delete_slot"

		// Create first
		_, err := mgr.CreateReplicationSlot(context.TODO(), schema.ReplicationSlotMeta{
			Name: slotName,
			Type: "physical",
		})
		assert.NoError(err)

		// Delete
		slot, err := mgr.DeleteReplicationSlot(context.TODO(), slotName)
		assert.NoError(err)
		assert.NotNil(slot)
		assert.Equal(slotName, slot.Name)

		// Verify it's gone
		_, err = mgr.GetReplicationSlot(context.TODO(), slotName)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {
		_, err := mgr.DeleteReplicationSlot(context.TODO(), "non_existing_slot_xyz")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("DeleteEmptyName", func(t *testing.T) {
		_, err := mgr.DeleteReplicationSlot(context.TODO(), "")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})
}

////////////////////////////////////////////////////////////////////////////////
// REPLICATION SLOT ROUND TRIP TESTS

func Test_Manager_ReplicationSlotRoundTrip(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("CreateGetDelete", func(t *testing.T) {
		slotName := "test_roundtrip_slot"

		// Create
		created, err := mgr.CreateReplicationSlot(context.TODO(), schema.ReplicationSlotMeta{
			Name: slotName,
			Type: "physical",
		})
		assert.NoError(err)
		assert.NotNil(created)

		// Get
		got, err := mgr.GetReplicationSlot(context.TODO(), slotName)
		assert.NoError(err)
		assert.Equal(created.Name, got.Name)
		assert.Equal(created.Type, got.Type)
		assert.Equal("inactive", got.Status)

		// List should include it
		list, err := mgr.ListReplicationSlots(context.TODO(), schema.ReplicationSlotListRequest{})
		assert.NoError(err)
		found := false
		for _, s := range list.Body {
			if s.Name == slotName {
				found = true
				break
			}
		}
		assert.True(found, "slot should appear in list")

		// Delete
		deleted, err := mgr.DeleteReplicationSlot(context.TODO(), slotName)
		assert.NoError(err)
		assert.Equal(slotName, deleted.Name)

		// Should be gone
		_, err = mgr.GetReplicationSlot(context.TODO(), slotName)
		assert.ErrorIs(err, pg.ErrNotFound)
	})
}
