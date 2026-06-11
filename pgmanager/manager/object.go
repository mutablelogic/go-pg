package manager

import (
	"context"
	"strings"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - OBJECT

func (manager *Manager) ListObjects(ctx context.Context, req schema.ObjectListRequest) (_ *schema.ObjectList, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "ListObjects",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// Set limit lower if request limit is lower
	var offset, limit uint64
	limit = schema.ObjectListLimit
	if req.Limit != nil && types.Value(req.Limit) < limit {
		limit = types.Value(req.Limit)
	}

	// Allocate the body with capacity
	var list schema.ObjectList
	list.Body = make([]schema.Object, 0, limit)

	// Iterate through all the databases
	if _, err := manager.withDatabases(ctx, func(database *schema.Database) error {
		// Filter by database
		if name := strings.TrimSpace(types.Value(req.Database)); name != "" && name != database.Name {
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

func (manager *Manager) GetObject(ctx context.Context, database, namespace, name string) (_ *schema.Object, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "GetObject",
		attribute.String("database", database),
		attribute.String("namespace", namespace),
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	// Validate input
	if database == "" {
		return nil, pg.ErrBadParameter.With("database is empty")
	}
	if namespace == "" {
		return nil, pg.ErrBadParameter.With("namespace is empty")
	}
	if name == "" {
		return nil, pg.ErrBadParameter.With("name is empty")
	}

	// Get the object
	var response schema.Object
	if err := manager.conn.Remote(database).With("as", schema.ObjectDef).Get(ctx, &response, schema.ObjectName{Schema: namespace, Name: name}); err != nil {
		return nil, err
	}
	return &response, nil
}
