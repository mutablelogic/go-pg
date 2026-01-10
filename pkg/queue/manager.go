package queue

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	sql "github.com/mutablelogic/go-pg/pkg/queue/sql"
	logger "github.com/mutablelogic/go-server/pkg/logger"
	ref "github.com/mutablelogic/go-server/pkg/ref"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	ns   string
	conn pg.PoolConn
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a new queue manager. The namespace parameter is used to scope all queue operations.
func New(ctx context.Context, conn pg.PoolConn, namespace string) (*Manager, error) {
	self := new(Manager)

	// Check namespace
	if namespace = strings.TrimSpace(namespace); namespace == "" {
		namespace = schema.SchemaName
	} else if namespace == schema.SchemaName {
		return nil, pg.ErrBadParameter.Withf("namespace %q is reserved for system use", schema.SchemaName)
	} else {
		self.ns = namespace
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
		self.conn = conn.WithQueries(queries).With("ns", namespace).(pg.PoolConn)
	}

	// Execute object SQL
	for _, key := range objects.Keys() {
		sql := objects.Get(key)
		if err := self.conn.Exec(ctx, sql); err != nil {
			return nil, err
		}
	}

	// Register cleanup ticker in system namespace if it doesn't exist
	var ticker schema.Ticker
	if err := self.conn.With("ns", schema.SchemaName).Get(ctx, &ticker, schema.TickerName(schema.CleanupTickerName)); errors.Is(err, pg.ErrNotFound) {
		// Ticker doesn't exist, create it
		cleanupInterval := schema.CleanupInterval
		if _, err := self.RegisterTickerNs(ctx, schema.SchemaName, schema.TickerMeta{
			Ticker:   schema.CleanupTickerName,
			Interval: &cleanupInterval,
		}); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	// Return success
	return self, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) Namespace() string {
	return manager.ns
}

func (manager *Manager) Conn() pg.PoolConn {
	return manager.conn
}

// ListNamespaces returns all distinct namespaces from the queue table
func (manager *Manager) ListNamespaces(ctx context.Context, req schema.NamespaceListRequest) (*schema.NamespaceList, error) {
	var list schema.NamespaceList
	if err := manager.conn.List(ctx, &list, req); err != nil {
		return nil, err
	}
	return &list, nil
}

// Run starts the background ticker loop for cleanup tasks in the system namespace.
// It runs until the context is cancelled. This should be called as a goroutine.
func (manager *Manager) Run(ctx context.Context) error {
	ch := make(chan *schema.Ticker, 1)
	defer close(ch)

	// Get the logger from the context
	log := ref.Log(ctx)
	if log == nil {
		log = logger.New(os.Stdout, logger.Text, false)
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
				// Run cleanup for all queues in this manager's namespace
				log.With("ticker", ticker.Ticker).Debug(ctx, "running cleanup")
				if err := manager.runCleanup(ctx); err != nil {
					log.Print(ctx, "cleanup error ", err)
				}
			}
		}
	}
}

// runCleanup cleans all queues across all namespaces
func (manager *Manager) runCleanup(ctx context.Context) error {
	// Get all distinct namespaces
	namespaces, err := manager.listNamespaces(ctx)
	if err != nil {
		return err
	}

	// Clean all queues in each namespace
	for _, ns := range namespaces {
		if err := manager.cleanNamespace(ctx, ns); err != nil {
			// Continue cleaning other namespaces even if one fails
			// TODO: Add proper logging
			_ = err
		}
	}

	return nil
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

// listNamespaces returns all distinct namespaces from the queue table
func (manager *Manager) listNamespaces(ctx context.Context) ([]string, error) {
	list, err := manager.ListNamespaces(ctx, schema.NamespaceListRequest{})
	if err != nil {
		return nil, err
	}
	return list.Body, nil
}
