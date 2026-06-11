package manager

import (
	"context"
	"slices"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - ROLES

// ListRoles returns a list of roles matching the request criteria.
// It supports pagination through the OffsetLimit fields in the request.
func (manager *Manager) ListRoles(ctx context.Context, req schema.RoleListRequest) (_ *schema.RoleList, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "ListRoles",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	var result schema.RoleList
	if err := manager.conn.List(ctx, &result, &req); err != nil {
		return nil, err
	}

	// Set the offset and limit in the result to reflect the actual count of items returned
	// which may be less than the requested limit if there are not enough items.
	result.RoleListRequest = req
	result.OffsetLimit.Clamp(result.Count)

	return &result, nil
}

// GetRole retrieves a single role by name.
// Returns an error if the name is empty or the role is not found.
func (manager *Manager) GetRole(ctx context.Context, name string) (_ *schema.Role, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "GetRole",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	// Validate input
	if name == "" {
		return nil, pg.ErrBadParameter.With("name is empty")
	}

	// Get the role
	var role schema.Role
	if err := manager.conn.Get(ctx, &role, schema.RoleName(name)); err != nil {
		return nil, err
	}
	return &role, nil
}

// CreateRole creates a new role with the specified metadata.
// The name must be a valid identifier and cannot have the reserved "pg_" prefix.
func (manager *Manager) CreateRole(ctx context.Context, meta schema.RoleMeta) (_ *schema.Role, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "CreateRole",
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	// Validate input
	if err := meta.Validate(); err != nil {
		return nil, err
	}

	// Insert the role and return it
	var role schema.Role
	if err := manager.conn.Insert(ctx, nil, meta); err != nil {
		return nil, err
	} else if err := manager.conn.Get(ctx, &role, schema.RoleName(meta.Name)); err != nil {
		return nil, err
	}

	return &role, nil
}

// DeleteRole deletes a role by name and returns the deleted role.
// Returns an error if the name is empty, has a reserved prefix, or the role is not found.
func (manager *Manager) DeleteRole(ctx context.Context, name string) (_ *schema.Role, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "DeleteRole",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	// Validate input
	if name == "" {
		return nil, pg.ErrBadParameter.With("name is empty")
	}

	// Get the role
	var role schema.Role
	if err := manager.conn.Get(ctx, &role, schema.RoleName(name)); err != nil {
		return nil, err
	} else if err := manager.conn.Delete(ctx, nil, schema.RoleName(name)); err != nil {
		return nil, err
	}

	return &role, nil
}

// UpdateRole updates an existing role with the specified metadata.
// If meta.Name is set and different from the current name, the role is renamed.
// If meta.Groups is set (even if empty), the group memberships are updated.
func (manager *Manager) UpdateRole(ctx context.Context, name string, meta schema.RoleMeta) (_ *schema.Role, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "UpdateRole",
		attribute.String("name", name),
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	// Validate input
	if name == "" {
		return nil, pg.ErrBadParameter.With("name is empty")
	}

	// Determine the final name for the role
	newName := name
	if meta.Name != "" && meta.Name != name {
		// Validate the new name
		if err := meta.Validate(); err != nil {
			return nil, err
		}
		newName = meta.Name
	} else {
		meta.Name = name
	}

	var role schema.Role
	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		// Get the role and memberships
		if err := manager.conn.Get(ctx, &role, schema.RoleName(name)); err != nil {
			return err
		}

		// Update the name if it's different
		if newName != name {
			if err := conn.Update(ctx, nil, schema.RoleName(newName), schema.RoleName(name)); err != nil {
				return err
			}
		}

		// Update the rest of the metadata
		if err := conn.Update(ctx, nil, meta, meta); err != nil {
			return err
		}

		// Update the group memberships
		if meta.Groups != nil {
			// Remove the old roles
			for _, oldrole := range role.Groups {
				if !slices.Contains(meta.Groups, oldrole) {
					if err := schema.RevokeGroupMembership(ctx, conn, oldrole, meta.Name); err != nil {
						return err
					}
				}
			}
			// Add the new roles
			for _, newrole := range meta.Groups {
				if !slices.Contains(role.Groups, newrole) {
					if err := schema.GrantGroupMembership(ctx, conn, newrole, meta.Name); err != nil {
						return err
					}
				}
			}
		}

		// Return success
		return nil
	}); err != nil {
		return nil, err
	}

	// Get the updated role
	if err := manager.conn.Get(ctx, &role, schema.RoleName(newName)); err != nil {
		return nil, err
	}

	// Return success
	return &role, nil
}
