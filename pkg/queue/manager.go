package queue

import (
	"context"
	"strings"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	sql "github.com/mutablelogic/go-pg/pkg/queue/sql"
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

	// Check parameters
	if conn == nil {
		return nil, pg.ErrBadParameter.With("connection is nil")
	}
	if namespace = strings.TrimSpace(namespace); namespace == "" {
		namespace = schema.SchemaName
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

	// Create connection with queries and namespace
	self.conn = conn.WithQueries(queries).With(
		"ns", namespace,
	).(pg.PoolConn)

	// Set the namespace
	self.ns = namespace

	// Execute object SQL
	for _, key := range objects.Keys() {
		sql := objects.Get(key)
		if err := self.conn.Exec(ctx, sql); err != nil {
			return nil, err
		}
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
