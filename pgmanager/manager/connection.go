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

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - CONNECTION

// ListConnections returns a list of active database connections matching the request criteria.
// It supports filtering by database, role, and state, as well as pagination.
func (manager *Manager) ListConnections(ctx context.Context, req schema.ConnectionListRequest) (_ *schema.ConnectionList, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "ListConnections",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// List connections
	var result schema.ConnectionList
	if err := manager.conn.List(ctx, &result, &req); err != nil {
		return nil, err
	}

	// Set the offset and limit in the result to reflect the actual count of items returned
	// which may be less than the requested limit if there are not enough items in the database.
	result.ConnectionListRequest = req
	result.OffsetLimit.Clamp(result.Count)

	// Return success
	return &result, nil
}

// GetConnection retrieves a single connection by process ID.
// Returns an error if the pid is zero or the connection is not found.
func (manager *Manager) GetConnection(ctx context.Context, pid uint64) (_ *schema.Connection, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "GetConnection",
		attribute.Int64("pid", int64(pid)),
	)
	defer func() { endSpan(err) }()

	// Validate input
	if pid == 0 {
		return nil, pg.ErrBadParameter.With("pid is zero")
	}

	// Get the connection
	var response schema.Connection
	if err := manager.conn.Get(ctx, &response, schema.ConnectionPid(pid)); err != nil {
		return nil, err
	}

	// Return success
	return &response, nil
}

// DeleteConnection terminates a connection by process ID and returns the terminated connection.
// Returns an error if the pid is zero or the connection is not found.
func (manager *Manager) DeleteConnection(ctx context.Context, pid uint64) (_ *schema.Connection, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "DeleteConnection",
		attribute.Int64("pid", int64(pid)),
	)
	defer func() { endSpan(err) }()

	// Validate input
	if pid == 0 {
		return nil, pg.ErrBadParameter.With("pid is zero")
	}

	// Delete the connection, unless it's this one
	var connection schema.Connection
	if err := notCurrent(ctx, manager.conn, pid, func(conn pg.Conn) error {
		return conn.Delete(ctx, &connection, schema.ConnectionPid(pid))
	}); err != nil {
		return nil, err
	}

	// Return success
	return &connection, nil
}

// CancelConnection cancels the query of a connection, by its pid, which
// stays connected, and returns it. It can't be this one.
func (manager *Manager) CancelConnection(ctx context.Context, pid uint64) (_ *schema.Connection, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "CancelConnection",
		attribute.Int64("pid", int64(pid)),
	)
	defer func() { endSpan(err) }()

	// Validate input
	if pid == 0 {
		return nil, pg.ErrBadParameter.With("pid is zero")
	}

	// Cancel its query, unless it's this one
	var connection schema.Connection
	if err := notCurrent(ctx, manager.conn, pid, func(conn pg.Conn) error {
		return conn.Get(ctx, &connection, schema.ConnectionCancel(pid))
	}); err != nil {
		return nil, err
	}

	// Return success
	return &connection, nil
}

// notCurrent calls the function, which ends, or cancels, a connection, by
// its pid, unless it's the connection which does so, which is a conflict.
// They're in a transaction, which keeps the one connection, as which is
// the current one is the connection's own.
func notCurrent(ctx context.Context, conn pg.PoolConn, pid uint64, fn func(pg.Conn) error) error {
	return conn.Tx(ctx, func(conn pg.Conn) error {
		var connection schema.Connection
		if err := conn.Get(ctx, &connection, schema.ConnectionPid(pid)); err != nil {
			return err
		} else if connection.Current {
			return pg.ErrConflict.Withf("connection %d is pgmanager's own, so it can't be ended, or cancelled", pid)
		}
		return fn(conn)
	})
}
