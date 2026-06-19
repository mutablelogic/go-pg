package manager

import (
	"context"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pggraph/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterGraph creates a new graph, or updates an existing graph, and returns it.
func (manager *Manager) RegisterGraph(ctx context.Context, name string, meta schema.GraphMeta) (_ *schema.Graph, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "RegisterGraph",
		attribute.String("name", name),
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	var graph schema.Graph
	if err := manager.queue.Tx(ctx, func(conn pg.Conn) error {
		return conn.With("name", name, "version", manager.version).Insert(ctx, &graph, meta)
	}); err != nil {
		return nil, err
	}

	// Return success
	return types.Ptr(graph), nil
}
