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
// PUBLIC METHODS - EXTENSION

// ListExtensions returns a list of extensions.
// If Database is specified, shows extensions for that specific database (with installed status).
// If Database is not specified, shows available extensions cluster-wide from the current connection.
// Use the Installed filter to show only installed, only not-installed, or all extensions.
func (manager *Manager) ListExtensions(ctx context.Context, req schema.ExtensionListRequest) (*schema.ExtensionList, error) {
	var list schema.ExtensionList

	// Determine which database to query
	database := strings.TrimSpace(types.PtrString(req.Database))
	if database != "" {
		// Query specific database via Remote
		if err := manager.conn.Remote(database).With("as", schema.ExtensionDef).List(ctx, &list, req); err != nil {
			return nil, err
		}

		// Set the database on each extension
		for i := range list.Body {
			list.Body[i].Database = database
		}
	} else {
		// No database specified - query directly for cluster-wide available extensions
		if err := manager.conn.List(ctx, &list, req); err != nil {
			return nil, err
		}
	}

	// Return success
	return &list, nil
}

func (manager *Manager) GetExtension(ctx context.Context, name string) (*schema.Extension, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, pg.ErrBadParameter.With("name is empty")
	}

	// Query directly for extension info (cluster-wide)
	var ext schema.Extension
	if err := manager.conn.Get(ctx, &ext, schema.ExtensionName(name)); err != nil {
		return nil, err
	}

	// Return success
	return &ext, nil
}

// CreateExtension installs an extension in a database.
// The Database field in meta specifies which database to install into.
// If cascade is true, dependent extensions are also installed.
func (manager *Manager) CreateExtension(ctx context.Context, meta schema.ExtensionMeta, cascade bool) (*schema.Extension, error) {
	// Check parameters
	database := strings.TrimSpace(meta.Database)
	if database == "" {
		return nil, pg.ErrBadParameter.With("database is empty")
	}
	name := strings.TrimSpace(meta.Name)
	if name == "" {
		return nil, pg.ErrBadParameter.With("name is empty")
	}

	// Create the extension
	conn := manager.conn.Remote(database).With("cascade", cascade)
	if err := conn.Insert(ctx, nil, meta); err != nil {
		return nil, err
	}

	// Get the extension
	var ext schema.Extension
	if err := conn.With("as", schema.ExtensionDef).Get(ctx, &ext, schema.ExtensionName(name)); err != nil {
		return nil, err
	} else {
		ext.Database = database
	}

	// Return success
	return &ext, nil
}

// UpdateExtension updates an extension's version and/or schema.
// The Database field in meta specifies which database to update.
// The Version field specifies the target version (empty means latest).
// The Schema field specifies a new schema to move the extension to (only for relocatable extensions).
// Note: Name and Owner cannot be changed for extensions in PostgreSQL.
func (manager *Manager) UpdateExtension(ctx context.Context, name string, meta schema.ExtensionMeta) (*schema.Extension, error) {
	// Check parameters
	database := strings.TrimSpace(meta.Database)
	if database == "" {
		return nil, pg.ErrBadParameter.With("database is empty")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, pg.ErrBadParameter.With("name is empty")
	}

	// Name and Owner cannot be changed
	if meta.Name != "" {
		return nil, pg.ErrBadParameter.With("name cannot be changed")
	}
	if meta.Owner != "" {
		return nil, pg.ErrBadParameter.With("owner cannot be changed")
	}

	conn := manager.conn.Remote(database)

	// If schema change is requested, first check if the extension is relocatable
	if meta.Schema != "" {
		var ext schema.Extension
		if err := conn.With("as", schema.ExtensionDef).Get(ctx, &ext, schema.ExtensionName(name)); err != nil {
			return nil, err
		}
		if ext.Relocatable == nil || !*ext.Relocatable {
			return nil, pg.ErrBadParameter.Withf("extension %q is not relocatable", name)
		}
	}

	// Update version if specified (or update to latest if no schema change)
	if meta.Version != "" || meta.Schema == "" {
		versionMeta := schema.ExtensionMeta{Name: name, Version: meta.Version}
		if err := conn.Update(ctx, nil, schema.ExtensionName(name), versionMeta); err != nil {
			return nil, err
		}
	}

	// Change schema if specified (separate ALTER EXTENSION ... SET SCHEMA)
	if meta.Schema != "" {
		schemaMeta := schema.ExtensionMeta{Name: name, Schema: meta.Schema}
		if err := conn.Update(ctx, nil, schema.ExtensionName(name), schemaMeta); err != nil {
			return nil, err
		}
	}

	// Get the extension
	var ext schema.Extension
	if err := conn.With("as", schema.ExtensionDef).Get(ctx, &ext, schema.ExtensionName(name)); err != nil {
		return nil, err
	} else {
		ext.Database = database
	}

	// Return success
	return &ext, nil
}

func (manager *Manager) DeleteExtension(ctx context.Context, database, name string, cascade bool) error {
	// Check parameters
	database = strings.TrimSpace(database)
	if database == "" {
		return pg.ErrBadParameter.With("database is required")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return pg.ErrBadParameter.With("name is empty")
	}

	// Delete the extension
	conn := manager.conn.Remote(database).With("cascade", cascade)
	if err := conn.Delete(ctx, nil, schema.ExtensionName(name)); err != nil {
		return err
	}

	// Return success
	return nil
}
