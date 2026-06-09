package manager

import (
	"context"
	"errors"
	"slices"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - SCHEMAS

// ListSchemas returns a list of schemas across all databases matching the request criteria.
// It supports pagination through the OffsetLimit fields in the request.
// If Database is specified in the request, only schemas from that database are returned.
func (manager *Manager) ListSchemas(ctx context.Context, req schema.SchemaListRequest) (*schema.SchemaList, error) {
	var list schema.SchemaList
	var offset, limit uint64

	// Set limit lower if request limit is lower
	limit = schema.SchemaListLimit
	if req.Limit != nil && types.PtrUint64(req.Limit) < limit {
		limit = types.PtrUint64(req.Limit)
	}

	// Allocate the body with capacity
	list.Body = make([]schema.Schema, 0, limit)

	// Iterate through all the databases
	if _, err := manager.withDatabases(ctx, func(database *schema.Database) error {
		// Filter by database
		if name := types.PtrString(req.Database); name != "" && name != database.Name {
			return nil
		}

		// Iterate through all the schemas
		count, err := manager.withSchemas(ctx, database.Name, func(s *schema.Schema) error {
			if offset >= req.Offset && uint64(len(list.Body)) < limit {
				list.Body = append(list.Body, *s)
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

// GetSchema retrieves a single schema by database and namespace name.
// Returns an error if the database or namespace is empty or the schema is not found.
func (manager *Manager) GetSchema(ctx context.Context, database, namespace string) (*schema.Schema, error) {
	if database == "" {
		return nil, pg.ErrBadParameter.With("database is empty")
	}
	if namespace == "" {
		return nil, pg.ErrBadParameter.With("namespace is empty")
	}
	var s schema.Schema
	if err := manager.conn.Remote(database).With("as", schema.SchemaDef).Get(ctx, &s, schema.SchemaName(namespace)); err != nil {
		return nil, err
	}
	return &s, nil
}

// CreateSchema creates a new schema in the specified database with the given metadata.
// ACL grants are applied after schema creation. If ACL grants fail, the schema is deleted
// to maintain consistency.
func (manager *Manager) CreateSchema(ctx context.Context, database string, meta schema.SchemaMeta) (*schema.Schema, error) {
	if database == "" {
		return nil, pg.ErrBadParameter.With("database is empty")
	}

	var s schema.Schema
	conn := manager.conn.Remote(database)

	// Create the schema
	if err := conn.Insert(ctx, nil, meta); err != nil {
		return nil, err
	}

	// Set ACL's
	if err := manager.conn.Tx(ctx, func(txConn pg.Conn) error {
		for _, acl := range meta.Acl {
			if err := acl.GrantSchema(ctx, conn, meta.Name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		// Delete the schema if there is an issue with ACL's
		deleteErr := conn.With("force", true).Delete(ctx, nil, schema.SchemaName(meta.Name))
		return nil, errors.Join(err, deleteErr)
	}

	// Get the schema
	if err := conn.With("as", schema.SchemaDef).Get(ctx, &s, schema.SchemaName(meta.Name)); err != nil {
		return nil, err
	}

	// Return success
	return &s, nil
}

// DeleteSchema drops a schema by database and namespace name, returning its metadata before deletion.
// If force is true, the schema is dropped with CASCADE even if there are dependent objects.
func (manager *Manager) DeleteSchema(ctx context.Context, database, namespace string, force bool) (*schema.Schema, error) {
	if database == "" {
		return nil, pg.ErrBadParameter.With("database is empty")
	}
	if namespace == "" {
		return nil, pg.ErrBadParameter.With("namespace is empty")
	}

	var s schema.Schema
	conn := manager.conn.Remote(database)

	// Get the schema
	if err := conn.With("as", schema.SchemaDef).Get(ctx, &s, schema.SchemaName(namespace)); err != nil {
		return nil, err
	}

	// Delete the schema
	if err := conn.With("force", force).Delete(ctx, nil, schema.SchemaName(namespace)); err != nil {
		return nil, err
	}

	// Return success
	return &s, nil
}

// UpdateSchema modifies an existing schema's metadata including name, owner, and ACLs.
// If meta.Name is provided and differs from namespace, the schema is renamed.
// ACL changes are synchronized by revoking removed privileges and granting new ones.
func (manager *Manager) UpdateSchema(ctx context.Context, database, namespace string, meta schema.SchemaMeta) (*schema.Schema, error) {
	if database == "" {
		return nil, pg.ErrBadParameter.With("database is empty")
	}
	if namespace == "" {
		return nil, pg.ErrBadParameter.With("namespace is empty")
	}

	var s schema.Schema
	conn := manager.conn.Remote(database)

	// Get the schema
	if err := conn.With("as", schema.SchemaDef).Get(ctx, &s, schema.SchemaName(namespace)); err != nil {
		return nil, err
	}

	// Update the name if it's different
	if meta.Name != "" && namespace != meta.Name {
		if err := conn.Update(ctx, nil, schema.SchemaName(meta.Name), schema.SchemaName(namespace)); err != nil {
			return nil, err
		}
	} else {
		meta.Name = namespace
	}

	// Update the owner if provided and different
	if meta.Owner != "" && s.Owner != meta.Owner {
		if err := conn.Update(ctx, nil, meta, meta); err != nil {
			return nil, err
		}
	}

	// Update ACL's
	if meta.Acl != nil {
		if err := manager.updateSchemaACLs(ctx, conn, meta.Name, s.Acl, meta.Acl); err != nil {
			return nil, err
		}
	}

	// Get the updated schema
	if err := conn.With("as", schema.SchemaDef).Get(ctx, &s, schema.SchemaName(meta.Name)); err != nil {
		return nil, err
	}

	// Return success
	return &s, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// updateSchemaACLs synchronizes ACLs between the current and desired state.
// It performs the following operations:
//   - Revokes all privileges for roles that are no longer in the desired list
//   - For existing roles, revokes privileges that were removed and grants new ones
//   - Grants all privileges for new roles
func (manager *Manager) updateSchemaACLs(ctx context.Context, conn pg.Conn, schemaName string, current, desired schema.ACLList) error {
	// Process existing ACLs
	for _, acl := range current {
		role := desired.Find(acl.Role)
		if role == nil {
			// Revoke all privileges for this role
			if err := acl.RevokeSchema(ctx, conn, schemaName); err != nil {
				return err
			}
			continue
		}

		// Check if privileges are the same
		if slices.Equal(acl.Priv, role.Priv) {
			continue
		}

		// If new role has ALL, just grant it
		if role.IsAll() {
			if err := role.GrantSchema(ctx, conn, schemaName); err != nil {
				return err
			}
			continue
		}

		// Revoke privileges that are no longer needed
		for _, priv := range acl.Priv {
			if !slices.Contains(role.Priv, priv) {
				if err := acl.WithPriv(priv).RevokeSchema(ctx, conn, schemaName); err != nil {
					return err
				}
			}
		}

		// Grant new privileges
		for _, priv := range role.Priv {
			if !slices.Contains(acl.Priv, priv) {
				if err := acl.WithPriv(priv).GrantSchema(ctx, conn, schemaName); err != nil {
					return err
				}
			}
		}
	}

	// Grant privileges for new roles
	for _, acl := range desired {
		if current.Find(acl.Role) == nil {
			if err := acl.GrantSchema(ctx, conn, schemaName); err != nil {
				return err
			}
		}
	}

	return nil
}
