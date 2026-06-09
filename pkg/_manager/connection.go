package manager

import (
	"context"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - CONNECTION

// ListConnections returns a list of active database connections matching the request criteria.
// It supports filtering by database, role, and state, as well as pagination.
func (manager *Manager) ListConnections(ctx context.Context, req schema.ConnectionListRequest) (*schema.ConnectionList, error) {
	var list schema.ConnectionList
	if err := manager.conn.List(ctx, &list, req); err != nil {
		return nil, err
	} else {
		return &list, nil
	}
}

// GetConnection retrieves a single connection by process ID.
// Returns an error if the pid is zero or the connection is not found.
func (manager *Manager) GetConnection(ctx context.Context, pid uint64) (*schema.Connection, error) {
	if pid == 0 {
		return nil, pg.ErrBadParameter.With("pid is zero")
	}
	var response schema.Connection
	if err := manager.conn.Get(ctx, &response, schema.ConnectionPid(pid)); err != nil {
		return nil, err
	}
	return &response, nil
}

// DeleteConnection terminates a connection by process ID and returns the terminated connection.
// Returns an error if the pid is zero or the connection is not found.
func (manager *Manager) DeleteConnection(ctx context.Context, pid uint64) (*schema.Connection, error) {
	if pid == 0 {
		return nil, pg.ErrBadParameter.With("pid is zero")
	}
	var connection schema.Connection
	if err := manager.conn.Delete(ctx, &connection, schema.ConnectionPid(pid)); err != nil {
		return nil, err
	}
	return &connection, nil
}
