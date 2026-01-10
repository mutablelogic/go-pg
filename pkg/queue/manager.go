package queue

import (
	"context"
	"errors"
	"strings"
	"sync"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	sql "github.com/mutablelogic/go-pg/pkg/queue/sql"
	ref "github.com/mutablelogic/go-server/pkg/ref"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	opts
	conn pg.PoolConn
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a new queue manager. Use WithNamespace to set the namespace for all queue operations.
func New(ctx context.Context, conn pg.PoolConn, opts ...Opt) (*Manager, error) {
	var result error
	self := new(Manager)

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

// Run starts the background ticker loop for cleanup tasks in the system namespace.
// It runs until the context is cancelled. This should be called as a goroutine.
func (manager *Manager) Run(ctx context.Context) error {
	ch := make(chan *schema.Ticker, 1)
	defer close(ch)

	// Get the logger from the context, or create one
	log := ref.Log(ctx)
	if log == nil {
		return errors.New("no logger in context")
	}

	// Start ticker loop in a goroutine
	var wg sync.WaitGroup
	errCh := make(chan error, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := manager.RunTickerLoopNs(ctx, schema.SchemaName, ch, schema.TickerPeriod); err != nil {
			errCh <- err
		}
	}()

	// Ensure goroutine is cleaned up on exit
	defer wg.Wait()

	// Process tickers
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			return err
		case ticker := <-ch:
			if ticker.Ticker == schema.CleanupTickerName {
				var result error

				// Start the span
				ctx, endspan := otel.StartSpan(manager.tracer, ctx, spanManagerName("cleanup"),
					attribute.String("ticker", ticker.String()),
				)

				// Run cleanup for all queues in this manager's namespace
				log.With("ticker", ticker.Ticker).Print(ctx, "running cleanup")
				if result = manager.cleanNamespace(ctx, manager.ns); result != nil {
					log.Print(ctx, "cleanup error ", result)
				}

				// End the span
				endspan(result)
			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

var SpanNameManager = "pgqueue.manager"

func spanManagerName(op string) string {
	return SpanNameManager + "." + op
}

// cleanNamespace cleans all queues in a specific namespace
func (manager *Manager) cleanNamespace(ctx context.Context, namespace string) error {
	var result error

	// List all queues in this namespace
	var queues schema.QueueList
	if err := manager.conn.With("ns", namespace).List(ctx, &queues, schema.QueueListRequest{}); err != nil {
		return err
	}

	// Clean each queue in this namespace
	for _, queue := range queues.Body {
		var resp schema.QueueCleanResponse
		if err := manager.conn.With("ns", namespace).List(ctx, &resp, schema.QueueCleanRequest{Queue: queue.Queue}); err != nil {
			// Continue cleaning other queues even if one fails
			result = errors.Join(result, err)
		}
	}

	// Return any errors
	return result
}
