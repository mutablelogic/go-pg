package manager

import (
	"context"
	"errors"
	"slices"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - DATABASES

// ListDatabases returns a list of databases matching the request criteria.
// It supports pagination through the OffsetLimit fields in the request.
func (manager *Manager) ListDatabases(ctx context.Context, req schema.DatabaseListRequest) (_ *schema.DatabaseList, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "ListDatabases",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	// List databases
	var result schema.DatabaseList
	if err := manager.conn.List(ctx, &result, req); err != nil {
		return nil, err
	}

	// Set the offset and limit in the result to reflect the actual count of items returned
	// which may be less than the requested limit if there are not enough items in the database.
	result.OffsetLimit = req.OffsetLimit
	result.OffsetLimit.Clamp(result.Count)

	// Return success
	return &result, nil
}

// GetDatabase retrieves a single database by name.
// Returns an error if the name is empty or the database is not found.
func (manager *Manager) GetDatabase(ctx context.Context, name string) (_ *schema.Database, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "GetDatabase",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	// Validate input
	if name == "" {
		return nil, pg.ErrBadParameter.With("name is empty")
	}

	// Get the database
	var database schema.Database
	if err := manager.conn.Get(ctx, &database, schema.DatabaseName(name)); err != nil {
		return nil, err
	}
	return &database, nil
}

// CreateDatabase creates a new database with the specified metadata.
// The database creation cannot be done in a transaction, but ACL grants are
// applied within a transaction. If ACL grants fail, the database is deleted
// to maintain consistency.
func (manager *Manager) CreateDatabase(ctx context.Context, meta schema.DatabaseMeta) (_ *schema.Database, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "CreateDatabase",
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	// Validate metadata
	if err := meta.Validate(); err != nil {
		return nil, err
	}

	// Create the database - cannot be done in a transaction
	var database schema.Database
	if err := manager.conn.Insert(ctx, nil, meta); err != nil {
		return nil, err
	}

	// Set ACL's - this can be done in a transaction
	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		for _, acl := range meta.Acl {
			if err := acl.GrantDatabase(ctx, conn, meta.Name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		// Delete the database if there is an issue with ACL's
		return nil, errors.Join(err, manager.conn.Delete(ctx, nil, schema.DatabaseName(meta.Name)))
	}

	// Get the database
	if err := manager.conn.Get(ctx, &database, schema.DatabaseName(meta.Name)); err != nil {
		return nil, err
	}

	// Return success
	return &database, nil
}

// DeleteDatabase drops a database by name and returns its metadata before deletion.
// If force is true, the database is dropped even if there are active connections.
func (manager *Manager) DeleteDatabase(ctx context.Context, name string, force bool) (_ *schema.Database, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "DeleteDatabase",
		attribute.String("name", name),
		attribute.Bool("force", force),
	)
	defer func() { endSpan(err) }()

	// Validate input
	if name == "" {
		return nil, pg.ErrBadParameter.With("name is empty")
	}

	// Get the database and delete it
	var database schema.Database
	if err := manager.conn.Get(ctx, &database, schema.DatabaseName(name)); err != nil {
		return nil, err
	} else if err := manager.conn.With("force", force).Delete(ctx, nil, schema.DatabaseName(name)); err != nil {
		return nil, err
	}

	// Return success
	return &database, nil
}

// UpdateDatabase modifies an existing database's metadata including name, owner, and ACLs.
// All changes are applied within a transaction to ensure atomicity.
// If meta.Name is provided and differs from name, the database is renamed.
// ACL changes are synchronized by revoking removed privileges and granting new ones.
func (manager *Manager) UpdateDatabase(ctx context.Context, name string, meta schema.DatabaseMeta) (_ *schema.Database, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "UpdateDatabase",
		attribute.String("name", name),
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	// Validate
	if name == "" {
		return nil, pg.ErrBadParameter.With("name is empty")
	}
	if meta.Name != "" {
		if err := (schema.DatabaseMeta{Name: meta.Name, Owner: meta.Owner}).Validate(); err != nil {
			return nil, err
		}
	} else if meta.Owner != "" {
		// Validate owner if provided
		if err := (schema.DatabaseMeta{Name: name, Owner: meta.Owner}).Validate(); err != nil {
			return nil, err
		}
	}

	// Update the database and ACL's in a transaction
	var database schema.Database
	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		// Get the database and ACL's
		if err := conn.Get(ctx, &database, schema.DatabaseName(name)); err != nil {
			return err
		}

		// Update the name if it's different
		if meta.Name != "" && name != meta.Name {
			if err := conn.Update(ctx, nil, schema.DatabaseName(meta.Name), schema.DatabaseName(name)); err != nil {
				return err
			}
		} else {
			meta.Name = name
		}

		// Update the rest of the metadata
		if err := conn.Update(ctx, nil, meta, meta); err != nil {
			return err
		}

		// Update ACL's
		if meta.Acl != nil {
			if err := manager.updateDatabaseACLs(ctx, conn, meta.Name, database.Acl, meta.Acl); err != nil {
				return err
			}
		}

		// Return success
		return nil
	}); err != nil {
		return nil, err
	}

	// Get the database
	if err := manager.conn.Get(ctx, &database, schema.DatabaseName(meta.Name)); err != nil {
		return nil, err
	}

	// Return success
	return &database, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// updateDatabaseACLs synchronizes ACLs between the current and desired state.
// It performs the following operations:
//   - Revokes all privileges for roles that are no longer in the desired list
//   - For existing roles, revokes privileges that were removed and grants new ones
//   - Grants all privileges for new roles
func (manager *Manager) updateDatabaseACLs(ctx context.Context, conn pg.Conn, dbName string, current, desired schema.ACLList) error {
	// Process existing ACLs
	for _, acl := range current {
		role := desired.Find(acl.Role)
		if role == nil {
			// Revoke all privileges for this role
			if err := acl.RevokeDatabase(ctx, conn, dbName); err != nil {
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
			if err := role.GrantDatabase(ctx, conn, dbName); err != nil {
				return err
			}
			continue
		}

		// Revoke privileges that are no longer needed
		for _, priv := range acl.Priv {
			if !slices.Contains(role.Priv, priv) {
				if err := acl.WithPriv(priv).RevokeDatabase(ctx, conn, dbName); err != nil {
					return err
				}
			}
		}

		// Grant new privileges
		for _, priv := range role.Priv {
			if !slices.Contains(acl.Priv, priv) {
				if err := acl.WithPriv(priv).GrantDatabase(ctx, conn, dbName); err != nil {
					return err
				}
			}
		}
	}

	// Grant privileges for new roles
	for _, acl := range desired {
		if current.Find(acl.Role) == nil {
			if err := acl.GrantDatabase(ctx, conn, dbName); err != nil {
				return err
			}
		}
	}

	return nil
}
