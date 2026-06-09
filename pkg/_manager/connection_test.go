package manager_test

import (
	"context"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pkg/manager"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	assert "github.com/stretchr/testify/assert"
)

////////////////////////////////////////////////////////////////////////////////
// LIST CONNECTIONS TESTS

func Test_Manager_ListConnections(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("ListAll", func(t *testing.T) {
		connections, err := mgr.ListConnections(context.TODO(), schema.ConnectionListRequest{})
		assert.NoError(err)
		assert.NotNil(connections)
		assert.Equal(len(connections.Body), int(connections.Count))
		// Should have at least one connection (our own)
		assert.GreaterOrEqual(connections.Count, uint64(1))
	})

	t.Run("ListWithPagination", func(t *testing.T) {
		limit := uint64(1)
		connections, err := mgr.ListConnections(context.TODO(), schema.ConnectionListRequest{
			OffsetLimit: pg.OffsetLimit{Limit: &limit},
		})
		assert.NoError(err)
		assert.NotNil(connections)
		assert.LessOrEqual(len(connections.Body), 1)
	})

	t.Run("ListByDatabase", func(t *testing.T) {
		dbName := "postgres"
		connections, err := mgr.ListConnections(context.TODO(), schema.ConnectionListRequest{
			Database: &dbName,
		})
		assert.NoError(err)
		assert.NotNil(connections)
		// All connections should be from postgres database
		for _, conn := range connections.Body {
			assert.Equal(dbName, conn.Database)
		}
	})

	t.Run("ListByState", func(t *testing.T) {
		state := "active"
		connections, err := mgr.ListConnections(context.TODO(), schema.ConnectionListRequest{
			State: &state,
		})
		assert.NoError(err)
		assert.NotNil(connections)
		// All connections should have active state
		for _, conn := range connections.Body {
			assert.Equal(state, conn.State)
		}
	})

	t.Run("ListWithMultipleFilters", func(t *testing.T) {
		dbName := "postgres"
		state := "active"
		connections, err := mgr.ListConnections(context.TODO(), schema.ConnectionListRequest{
			Database: &dbName,
			State:    &state,
		})
		assert.NoError(err)
		assert.NotNil(connections)
		// All connections should match both filters
		for _, conn := range connections.Body {
			assert.Equal(dbName, conn.Database)
			assert.Equal(state, conn.State)
		}
	})

	t.Run("ListWithOffset", func(t *testing.T) {
		// First get all connections
		allConnections, err := mgr.ListConnections(context.TODO(), schema.ConnectionListRequest{})
		assert.NoError(err)

		if allConnections.Count < 2 {
			t.Skip("Not enough connections to test offset")
		}

		// Get with offset
		limit := uint64(10)
		connections, err := mgr.ListConnections(context.TODO(), schema.ConnectionListRequest{
			OffsetLimit: pg.OffsetLimit{
				Limit:  &limit,
				Offset: 1,
			},
		})
		assert.NoError(err)
		assert.NotNil(connections)
		// Should have fewer connections than total
		assert.Less(len(connections.Body), int(allConnections.Count))
	})
}

////////////////////////////////////////////////////////////////////////////////
// GET CONNECTION TESTS

func Test_Manager_GetConnection(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("GetExisting", func(t *testing.T) {
		// First list connections to get a valid PID
		connections, err := mgr.ListConnections(context.TODO(), schema.ConnectionListRequest{})
		if !assert.NoError(err) || connections.Count == 0 {
			t.Skip("No connections available to test")
		}

		// Get the first connection's PID
		pid := uint64(connections.Body[0].Pid)
		connection, err := mgr.GetConnection(context.TODO(), pid)
		assert.NoError(err)
		assert.NotNil(connection)
		assert.Equal(uint32(pid), connection.Pid)
	})

	t.Run("GetNonExistent", func(t *testing.T) {
		// Use a very high PID that should not exist
		_, err := mgr.GetConnection(context.TODO(), 999999999)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("GetZeroPid", func(t *testing.T) {
		_, err := mgr.GetConnection(context.TODO(), 0)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})
}

////////////////////////////////////////////////////////////////////////////////
// DELETE CONNECTION TESTS

func Test_Manager_DeleteConnection(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("DeleteZeroPid", func(t *testing.T) {
		_, err := mgr.DeleteConnection(context.TODO(), 0)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	// Note: We don't test DeleteConnection with a valid PID here because
	// terminating connections during tests could be disruptive.
	// The schema-level tests verify the SQL generation is correct.
}
