package manager

import (
	"context"
	"errors"
	"slices"
	"strings"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - TABLESPACES

// ListTablespaces returns a list of tablespaces matching the request criteria.
// It supports pagination through the OffsetLimit fields in the request.
func (manager *Manager) ListTablespaces(ctx context.Context, req schema.TablespaceListRequest) (*schema.TablespaceList, error) {
	var list schema.TablespaceList
	if err := manager.conn.List(ctx, &list, req); err != nil {
		return nil, err
	} else {
		return &list, nil
	}
}

// GetTablespace retrieves a single tablespace by name.
// Returns an error if the name is empty or the tablespace is not found.
func (manager *Manager) GetTablespace(ctx context.Context, name string) (*schema.Tablespace, error) {
	if name == "" {
		return nil, pg.ErrBadParameter.With("name is empty")
	}
	var response schema.Tablespace
	if err := manager.conn.Get(ctx, &response, schema.TablespaceName(name)); err != nil {
		return nil, err
	}
	return &response, nil
}

// CreateTablespace creates a new tablespace with the specified metadata and location.
// The tablespace creation cannot be done in a transaction, but ACL grants are
// applied within a transaction. If ACL grants fail, the tablespace is deleted
// to maintain consistency.
func (manager *Manager) CreateTablespace(ctx context.Context, meta schema.TablespaceMeta, location string) (*schema.Tablespace, error) {
	var response schema.Tablespace

	// Create the tablespace (outside a transaction)
	if err := manager.conn.With("location", location).Insert(ctx, nil, meta); err != nil {
		return nil, err
	}

	// Set ACL's
	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		for _, acl := range meta.Acl {
			if err := acl.GrantTablespace(ctx, conn, meta.Name); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		// Delete the tablespace if there is an issue with ACL's
		deleteErr := manager.conn.Delete(ctx, nil, schema.TablespaceName(meta.Name))
		return nil, errors.Join(err, deleteErr)
	}

	// Get the tablespace
	if err := manager.conn.Get(ctx, &response, schema.TablespaceName(meta.Name)); err != nil {
		return nil, err
	}

	// Return success
	return &response, nil
}

// DeleteTablespace drops a tablespace by name and returns its metadata before deletion.
// Returns an error if the name is empty or the tablespace is not found.
func (manager *Manager) DeleteTablespace(ctx context.Context, name string) (*schema.Tablespace, error) {
	if name == "" {
		return nil, pg.ErrBadParameter.With("name is empty")
	}
	var response schema.Tablespace

	// Get the tablespace
	if err := manager.conn.Get(ctx, &response, schema.TablespaceName(name)); err != nil {
		return nil, err
	}

	// Delete the tablespace
	if err := manager.conn.Delete(ctx, nil, schema.TablespaceName(name)); err != nil {
		return nil, err
	}

	// Return success
	return &response, nil
}

// UpdateTablespace modifies an existing tablespace's metadata including name, owner, and ACLs.
// All changes are applied within a transaction to ensure atomicity.
// If meta.Name is provided and differs from name, the tablespace is renamed.
// ACL changes are synchronized by revoking removed privileges and granting new ones.
func (manager *Manager) UpdateTablespace(ctx context.Context, name string, meta schema.TablespaceMeta) (*schema.Tablespace, error) {
	if name == "" {
		return nil, pg.ErrBadParameter.With("name is empty")
	}
	var response schema.Tablespace

	// Get the tablespace
	if err := manager.conn.Get(ctx, &response, schema.TablespaceName(name)); err != nil {
		return nil, err
	}

	// Update in a transaction
	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		// Update the name if it's different
		if rename := strings.TrimSpace(meta.Name); rename != "" && name != rename {
			if err := conn.Update(ctx, nil, schema.TablespaceName(rename), schema.TablespaceName(name)); err != nil {
				return err
			} else {
				meta.Name = rename
			}
		} else {
			meta.Name = name
		}

		// Update the rest of the metadata
		if owner := strings.TrimSpace(meta.Owner); owner != "" && response.Owner != owner {
			if err := conn.Update(ctx, nil, meta, meta); err != nil {
				return err
			}
		}

		// Update ACL's
		if meta.Acl != nil {
			for _, acl := range response.Acl {
				if role := meta.Acl.Find(acl.Role); role == nil {
					// Revoke the older privileges
					if err := acl.RevokeTablespace(ctx, conn, meta.Name); err != nil {
						return err
					}
				} else if slices.Equal(acl.Priv, role.Priv) {
					// No change
				} else if role.IsAll() {
					// Just grant
					if err := role.GrantTablespace(ctx, conn, meta.Name); err != nil {
						return err
					}
				} else {
					// Revoke
					for _, priv := range acl.Priv {
						if !slices.Contains(role.Priv, priv) {
							if err := acl.WithPriv(priv).RevokeTablespace(ctx, conn, meta.Name); err != nil {
								return err
							}
						}
					}
					// Grant
					for _, priv := range role.Priv {
						if !slices.Contains(acl.Priv, priv) {
							if err := acl.WithPriv(priv).GrantTablespace(ctx, conn, meta.Name); err != nil {
								return err
							}
						}
					}
				}
			}

			// Create new privileges
			for _, acl := range meta.Acl {
				if role := response.Acl.Find(acl.Role); role == nil {
					if err := acl.GrantTablespace(ctx, conn, meta.Name); err != nil {
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

	// Get the tablespace
	if err := manager.conn.Get(ctx, &response, schema.TablespaceName(meta.Name)); err != nil {
		return nil, err
	}

	// Return success
	return &response, nil
}
