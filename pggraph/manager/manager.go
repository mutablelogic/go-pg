package manager

import (
	"context"
	"fmt"
	"strings"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pggraph/schema"
	pgqueue "github.com/mutablelogic/go-pg/pgqueue/manager"
	attribute "go.opentelemetry.io/otel/attribute"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	opt
	queue *pgqueue.Manager
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(ctx context.Context, pool pg.PoolConn, version string, opts ...Opt) (*Manager, error) {
	self := new(Manager)

	// Check arguments
	if pool == nil {
		return nil, fmt.Errorf("pool is required")
	}

	// Set default values
	if err := self.defaults(version); err != nil {
		return nil, err
	}

	// Apply options
	if err := self.apply(opts...); err != nil {
		return nil, err
	}

	// Parse and register named queries so bind.Query(...) can resolve them.
	queries, err := pg.NewQueries(strings.NewReader(schema.Queries))
	if err != nil {
		return nil, fmt.Errorf("parse queries.sql: %w", err)
	} else {
		pool = pool.WithQueries(queries).With(
			"schema", self.schema,
		).(pg.PoolConn)
	}

	// Create objects in the database schema. This is not done in a transaction
	bootstrapCtx, endBootstrapSpan := otel.StartSpan(self.tracer, ctx, "bootstrap",
		attribute.String("schema", self.schema),
	)
	if err := bootstrap(bootstrapCtx, pool, self.schema); err != nil {
		endBootstrapSpan(err)
		return nil, err
	}

	// Create the queue manager
	queue, err := pgqueue.New(ctx, pool, pgqueue.WithSchema(self.schema), pgqueue.WithTracer(self.tracer), pgqueue.WithMeter(self.metrics), pgqueue.WithWorker(self.worker))
	if err != nil {
		endBootstrapSpan(err)
		return nil, fmt.Errorf("create queue manager: %w", err)
	} else {
		self.queue = queue
	}

	// Return success
	endBootstrapSpan(nil)
	return self, nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func bootstrap(ctx context.Context, conn pg.Conn, schemaName string) error {
	// Get all objects
	objects, err := pg.NewQueries(strings.NewReader(schema.Objects))
	if err != nil {
		return fmt.Errorf("parse objects.sql: %w", err)
	}

	// Create the schema
	if err := pg.SchemaCreate(ctx, conn, schemaName); err != nil {
		return fmt.Errorf("create schema %q: %w", schemaName, err)
	}

	// Create all objects - not in a transaction
	for _, key := range objects.Keys() {
		if err := conn.Exec(ctx, objects.Query(key)); err != nil {
			return fmt.Errorf("create object %q: %w", key, err)
		}
	}

	// Return success
	return nil
}
