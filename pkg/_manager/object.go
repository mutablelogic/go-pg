package manager

import (
	"context"
	"strings"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - OBJECT

func (manager *Manager) ListObjects(ctx context.Context, req schema.ObjectListRequest) (*schema.ObjectList, error) {
	var list schema.ObjectList
	var offset, limit uint64

	// Set limit lower if request limit is lower
	limit = schema.ObjectListLimit
	if req.Limit != nil && types.PtrUint64(req.Limit) < limit {
		limit = types.PtrUint64(req.Limit)
	}

	// Allocate the body with capacity
	list.Body = make([]schema.Object, 0, limit)

	// Iterate through all the databases
	if _, err := manager.withDatabases(ctx, func(database *schema.Database) error {
		// Filter by database
		if name := strings.TrimSpace(types.PtrString(req.Database)); name != "" && name != database.Name {
			return nil
		}

		// Iterate through all the objects
		count, err := manager.withObjects(ctx, database.Name, req, func(object *schema.Object) error {
			if offset >= req.Offset && uint64(len(list.Body)) < limit {
				list.Body = append(list.Body, *object)
			}
			offset++
			return nil
		})
		if err != nil {
			return err
		}

		// Increment the count
		list.Count += count

		// Return success
		return nil
	}); err != nil {
		return nil, err
	}

	// Return success
	return &list, nil
}

func (manager *Manager) GetObject(ctx context.Context, database, namespace, name string) (*schema.Object, error) {
	if database == "" {
		return nil, pg.ErrBadParameter.With("database is empty")
	}
	if namespace == "" {
		return nil, pg.ErrBadParameter.With("namespace is empty")
	}
	if name == "" {
		return nil, pg.ErrBadParameter.With("name is empty")
	}
	var response schema.Object
	if err := manager.conn.Remote(database).With("as", schema.ObjectDef).Get(ctx, &response, schema.ObjectName{Schema: namespace, Name: name}); err != nil {
		return nil, err
	}
	return &response, nil
}
