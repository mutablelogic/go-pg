package manager

import (
	"context"

	"github.com/mutablelogic/go-client/pkg/otel"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Ping the database connection - returns an error if the connection is not healthy.
func (manager *Manager) Ping(ctx context.Context) (err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "Ping")
	defer func() { endSpan(err) }()

	// Perform the ping
	return manager.conn.Ping(ctx)
}
