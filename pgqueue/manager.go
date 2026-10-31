package pgqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

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

	// The task callbacks for the queues and tickers, by name
	queues  *exec
	tickers *exec

	// Held for reading while a task is retained and run, and for writing
	// while a queue's callback is removed, so a task isn't retained from a
	// queue whose callback is then missing
	dispatch sync.RWMutex

	// Set while the event loop is running, and once it's ready, which is
	// when it's listening for new tasks
	running atomic.Bool
	ready   atomic.Bool
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(ctx context.Context, pool pg.PoolConn, opts ...Opt) (_ *Manager, err error) {
	self := new(Manager)

	// Check parameters
	if pool == nil {
		return nil, pg.ErrBadParameter.With("pg pool is required")
	}

	// Apply options
	if err := self.opt.apply(opts); err != nil {
		return nil, err
	}

	// The task callbacks, which are traced with the tracer from the options
	self.queues = newExec(self.tracer)
	self.tickers = newExec(self.tracer)

	// Trace creating the manager, which needs the tracer from the options
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.New",
		attribute.String("schema", self.schema),
	)
	defer func() { endSpan(err) }()

	// Parse and register named queries so bind.Query(...) can resolve them.
	queries, err := pg.NewQueries(strings.NewReader(schema.Queries))
	if err != nil {
		return nil, pg.ErrInternalServerError.Withf("queries.sql: %w", err)
	} else {
		pool = pool.WithQueries(queries).With(
			"schema", self.schema,
			"channel", schema.DefaultNotifyChannel,
		).(pg.PoolConn)
	}

	// Create the schema and the objects in it
	if err := bootstrap(ctx, pool, self.schema); err != nil {
		return nil, err
	} else {
		self.PoolConn = pool
	}

	// Ensure at least one task partition exists before any task inserts happen.
	if _, err := self.CreateNextPartition(ctx); err != nil {
		return nil, err
	}

	// Register a maintenance ticker
	if _, err := self.RegisterTicker(ctx, schema.DefaultMaintenanceTickerName, schema.TickerMeta{
		Interval: types.Ptr(self.maintenancePeriod),
	}, self.maintenance); err != nil {
		return nil, err
	}

	// Register a cleanup ticker
	if _, err := self.RegisterTicker(ctx, schema.DefaultCleanupTickerName, schema.TickerMeta{
		Interval: types.Ptr(schema.DefaultCleanupPeriod),
	}, self.cleanup); err != nil {
		return nil, err
	}

	// Register the metrics
	if err := self.registerMetrics(); err != nil {
		return nil, err
	}

	// Return success
	return self, nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Running reports whether the event loop is running, and listening for new
// tasks, so a task created afterwards is run straight away
func (self *Manager) Running() bool {
	return self.ready.Load()
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

	// Create all objects - not in a transaction - in spans named by their keys
	for _, key := range objects.Keys() {
		if err := conn.With(pg.TraceSpanNameArg, key).Exec(ctx, objects.Query(key)); err != nil {
			return fmt.Errorf("create object %q: %w", key, err)
		}
	}

	// Return success
	return nil
}

func (self *Manager) maintenance(ctx context.Context, _ json.RawMessage) (any, error) {
	messages := []string{}

	// Create next partition if needed
	created, err := self.CreateNextPartition(ctx)
	if err != nil {
		return nil, err
	} else if created != "" {
		messages = append(messages, fmt.Sprintf("created partition %q", created))
	}

	// Drop old drained partitions
	dropped, err := self.DropDrainedPartition(ctx)
	if err != nil {
		return nil, err
	} else if dropped != "" {
		messages = append(messages, fmt.Sprintf("dropped partition %q", dropped))
	}

	// Return success
	return messages, nil
}

func (self *Manager) cleanup(ctx context.Context, _ json.RawMessage) (any, error) {
	messages := make([]string, 0)
	pageSize := uint64(schema.QueueListLimit)
	offset := uint64(0)

	for {
		pageLimit := pageSize
		request := schema.QueueListRequest{OffsetLimit: pg.OffsetLimit{Offset: offset, Limit: &pageLimit}}

		queues, err := self.ListQueues(ctx, request)
		if err != nil {
			return nil, err
		}

		// Clean each queue in batches, until a batch isn't full, so a busy
		// queue is fully cleaned
		for _, queue := range queues.Body {
			var count int
			for {
				removed, err := self.CleanQueue(ctx, queue.Name)
				if err != nil {
					return nil, err
				}
				count += len(removed)
				if len(removed) < schema.QueueCleanLimit {
					break
				}
			}
			if count > 0 {
				messages = append(messages, fmt.Sprintf("cleaned queue %q removed %d tasks", queue.Queue, count))
			}
		}

		if len(queues.Body) < int(pageSize) {
			break
		}
		offset += uint64(len(queues.Body))
	}

	return messages, nil
}
