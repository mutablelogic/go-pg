package queue

import (
	"context"
	"errors"
	"strings"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	sql "github.com/mutablelogic/go-pg/pkg/queue/sql"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	opts
	conn pg.PoolConn
	pool *workerPool
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a new queue manager. Use WithNamespace to set the namespace for all queue operations.
func New(ctx context.Context, conn pg.PoolConn, opts ...Opt) (*Manager, error) {
	var result error
	self := new(Manager)
	self.pool = newWorkerPool()

	// Apply options (includes namespace validation)
	if opt, err := applyOpts(opts); err != nil {
		return nil, err
	} else {
		self.opts = opt
	}

	// Parse query SQL
	queries, err := pg.NewQueries(strings.NewReader(sql.Queries))
	if err != nil {
		return nil, err
	}

	// Parse object SQL
	objects, err := pg.NewQueries(strings.NewReader(sql.Objects))
	if err != nil {
		return nil, err
	}

	// Check and set connection
	if conn == nil {
		return nil, pg.ErrBadParameter.With("connection is nil")
	} else {
		self.conn = conn.WithQueries(queries).With("ns", self.ns).(pg.PoolConn)
	}

	// Execute object SQL to create necessary tables, indexes, etc.
	ctx, endspan := otel.StartSpan(self.tracer, ctx, spanManagerName("new"))
	defer func() { endspan(result) }()

	// Iterate through object creation queries
	for _, key := range objects.Keys() {
		sql := objects.Query(key)
		if result = self.conn.With(pg.TraceSpanNameArg, key).Exec(ctx, sql); result != nil {
			return nil, result
		}
	}

	// Register cleanup ticker in system namespace if it doesn't exist
	var ticker schema.Ticker
	if err := self.conn.With("ns", schema.SchemaName).Get(ctx, &ticker, schema.TickerName(schema.CleanupTickerName)); errors.Is(err, pg.ErrNotFound) {
		// Ticker doesn't exist, create it
		cleanupInterval := schema.CleanupInterval
		if _, result := self.RegisterTickerNs(ctx, schema.SchemaName, schema.TickerMeta{
			Ticker:   schema.CleanupTickerName,
			Interval: &cleanupInterval,
		}); result != nil {
			return nil, result
		}
	} else if err != nil {
		result = err
	}

	// Return success
	return self, result
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) Namespace() string {
	return manager.ns
}

func (manager *Manager) Conn() pg.PoolConn {
	return manager.conn
}
