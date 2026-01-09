package manager

import (
	"context"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	conn pg.PoolConn

	// Feature flags
	statStatementsAvailable bool
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a new database manager.
func New(ctx context.Context, conn pg.PoolConn) (*Manager, error) {
	if conn == nil {
		return nil, pg.ErrBadParameter.With("connection is nil")
	}
	self := new(Manager)
	self.conn = conn.With("schema", schema.CatalogSchema).(pg.PoolConn)

	// Bootstrap extensions
	result, err := schema.Bootstrap(ctx, self.conn)
	if err != nil {
		return nil, err
	}
	self.statStatementsAvailable = result.StatStatementsAvailable

	// Return success
	return self, nil
}

// StatStatementsAvailable returns true if pg_stat_statements extension is available
func (manager *Manager) StatStatementsAvailable() bool {
	return manager.statStatementsAvailable
}

// Iterate through all the databases
func (manager *Manager) withDatabases(ctx context.Context, fn func(database *schema.Database) error) (uint64, error) {
	var req schema.DatabaseListRequest
	req.Offset = 0
	req.Limit = types.Uint64Ptr(schema.DatabaseListLimit)

	for {
		list, err := manager.ListDatabases(ctx, req)
		if err != nil {
			return 0, err
		}
		for _, database := range list.Body {
			if err := fn(&database); err != nil {
				return 0, err
			}
		}

		// Determine if the next page is over the count
		next := req.Offset + types.PtrUint64(req.Limit)
		if next >= list.Count {
			return list.Count, nil
		} else {
			req.Offset = next
		}
	}
}

// Iterate through all the schemas for a database
func (manager *Manager) withSchemas(ctx context.Context, database string, fn func(schema *schema.Schema) error) (uint64, error) {
	var req schema.SchemaListRequest
	req.Offset = 0
	req.Limit = types.Uint64Ptr(schema.SchemaListLimit)

	for {
		var list schema.SchemaList
		if err := manager.conn.Remote(database).With("as", schema.SchemaDef).List(ctx, &list, &req); err != nil {
			return 0, err
		}

		for _, schema := range list.Body {
			if err := fn(&schema); err != nil {
				return 0, err
			}
		}

		// Determine if the next page is over the count
		next := req.Offset + types.PtrUint64(req.Limit)
		if next >= list.Count {
			return list.Count, nil
		} else {
			req.Offset = next
		}
	}
}

// Iterate through all the objects for a database - requires object.go to be ported from go-server
func (manager *Manager) withObjects(ctx context.Context, database string, req schema.ObjectListRequest, fn func(schema *schema.Object) error) (uint64, error) {
	req.Offset = 0
	req.Limit = types.Uint64Ptr(schema.ObjectListLimit)

	for {
		var list schema.ObjectList
		if err := manager.conn.Remote(database).With("as", schema.ObjectDef).List(ctx, &list, &req); err != nil {
			return 0, err
		}

		for _, object := range list.Body {
			if err := fn(&object); err != nil {
				return 0, err
			}
		}

		// Determine if the next page is over the count
		next := req.Offset + types.PtrUint64(req.Limit)
		if next >= list.Count {
			return list.Count, nil
		} else {
			req.Offset = next
		}
	}
}
