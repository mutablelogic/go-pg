package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	opt
	pg.PoolConn
	queues  *exec
	tickers *exec
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(ctx context.Context, pool pg.PoolConn, opts ...Opt) (*Manager, error) {
	self := new(Manager)

	// Check arguments
	if pool == nil {
		return nil, fmt.Errorf("pool is required")
	}

	// Set default values
	if err := self.defaults(); err != nil {
		return nil, err
	}

	// Apply options
	if err := self.apply(opts...); err != nil {
		return nil, err
	}

	// Create execution objects after options are applied so they inherit the configured tracer.
	self.queues = NewExec(self.tracer)
	self.tickers = NewExec(self.tracer)

	// Parse and register named queries so bind.Query(...) can resolve them.
	queries, err := pg.NewQueries(strings.NewReader(schema.Queries))
	if err != nil {
		return nil, fmt.Errorf("parse queries.sql: %w", err)
	} else {
		pool = pool.WithQueries(queries).With(
			"schema", self.schema,
			"channel", schema.DefaultNotifyChannel,
		).(pg.PoolConn)
	}

	// Create objects in the database schema. This is not done in a transaction
	bootstrapCtx, endBootstrapSpan := otel.StartSpan(self.tracer, ctx, "bootstrap",
		attribute.String("schema", self.schema),
	)
	if err := bootstrap(bootstrapCtx, pool, self.schema); err != nil {
		endBootstrapSpan(err)
		return nil, err
	} else {
		self.PoolConn = pool
	}

	// Ensure at least one task partition exists before any task inserts happen.
	if _, err := self.CreateNextPartition(bootstrapCtx); err != nil {
		endBootstrapSpan(err)
		return nil, err
	}

	// Register a maintenance ticker
	if _, err := self.RegisterTicker(bootstrapCtx, schema.DefaultMaintenanceTickerName, schema.TickerMeta{
		Interval: types.Ptr(self.maintenancePeriod),
	}, self.maintenance); err != nil {
		endBootstrapSpan(err)
		return nil, err
	}

	// Register a cleanup ticker
	if _, err := self.RegisterTicker(bootstrapCtx, schema.DefaultCleanupTickerName, schema.TickerMeta{
		Interval: types.Ptr(schema.DefaultCleanupPeriod),
	}, self.cleanup); err != nil {
		endBootstrapSpan(err)
		return nil, err
	}

	// Register the metrics
	if err := self.registerMetrics(); err != nil {
		endBootstrapSpan(err)
		return nil, err
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

func (manager *Manager) maintenance(ctx context.Context, _ json.RawMessage) (any, error) {
	messages := []string{}

	// Create next partition if needed
	created, err := manager.CreateNextPartition(ctx)
	if err != nil {
		return nil, err
	} else if created != "" {
		messages = append(messages, fmt.Sprintf("created partition %q", created))
	}

	// Drop old drained partitions
	dropped, err := manager.DropDrainedPartition(ctx)
	if err != nil {
		return nil, err
	} else if dropped != "" {
		messages = append(messages, fmt.Sprintf("dropped partition %q", dropped))
	}

	// Return success
	return messages, nil
}

func (manager *Manager) cleanup(ctx context.Context, _ json.RawMessage) (any, error) {
	messages := make([]string, 0)
	pageSize := uint64(schema.QueueListLimit)
	offset := uint64(0)

	for {
		pageLimit := pageSize
		request := schema.QueueListRequest{OffsetLimit: pg.OffsetLimit{Offset: offset, Limit: &pageLimit}}

		queues, err := manager.ListQueues(ctx, request)
		if err != nil {
			return nil, err
		}

		for _, queue := range queues.Body {
			removed, err := manager.CleanQueue(ctx, queue.Queue)
			if err != nil {
				return nil, err
			}
			if len(removed) > 0 {
				messages = append(messages, fmt.Sprintf("cleaned queue %q removed %d tasks", queue.Queue, len(removed)))
			}
		}

		if len(queues.Body) < int(pageSize) {
			break
		}
		offset += uint64(len(queues.Body))
	}

	return messages, nil
}
