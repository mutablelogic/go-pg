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
	if err := manager.conn.List(ctx, &result, req); err != nil {
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

	// Delete the connection
	var connection schema.Connection
	if err := manager.conn.Delete(ctx, &connection, schema.ConnectionPid(pid)); err != nil {
		return nil, err
	}

	// Return success
	return &connection, nil
}
