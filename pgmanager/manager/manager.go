package manager

import (
	"context"
	"errors"
	"fmt"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	opts
	conn pg.PoolConn
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(conn pg.PoolConn, opt ...Opt) (*Manager, error) {
	self := new(Manager)

	// Set the schema
	self.conn = conn.With("schema", schema.CatalogSchema).(pg.PoolConn)

	// Set default options
	self.opts = opts{}

	// Get the cluster name for metrics
	var cluster schema.Cluster
	if err := self.conn.Get(context.Background(), &cluster, cluster); err != nil {
		return nil, fmt.Errorf("get cluster name: %w", err)
	} else {
		self.cluster = cluster.Name
	}

	// Apply options
	if err := self.opts.apply(opt...); err != nil {
		return nil, err
	}

	// Register metrics
	if self.metrics != nil {
		err := errors.Join(
			self.RegisterDatabaseMetrics("database"),
			self.RegisterSchemaMetrics("schema"),
			self.RegisterConnectionMetrics("connection"),
		)
		if err != nil {
			return nil, fmt.Errorf("register metrics: %w", err)
		}
	}

	// Return success
	return self, nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

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

// // Iterate through all the objects for a database - requires object.go to be ported from go-server
// func (manager *Manager) withObjects(ctx context.Context, database string, req schema.ObjectListRequest, fn func(schema *schema.Object) error) (uint64, error) {
// 	req.Offset = 0
// 	req.Limit = types.Uint64Ptr(schema.ObjectListLimit)

// 	for {
// 		var list schema.ObjectList
// 		if err := manager.conn.Remote(database).With("as", schema.ObjectDef).List(ctx, &list, &req); err != nil {
// 			return 0, err
// 		}

// 		for _, object := range list.Body {
// 			if err := fn(&object); err != nil {
// 				return 0, err
// 			}
// 		}

// 		// Determine if the next page is over the count
// 		next := req.Offset + types.PtrUint64(req.Limit)
// 		if next >= list.Count {
// 			return list.Count, nil
// 		} else {
// 			req.Offset = next
// 		}
// 	}
// }
