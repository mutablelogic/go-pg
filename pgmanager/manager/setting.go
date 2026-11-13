package manager

import (
	"context"
	"slices"
	"strings"
	"time"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListSettings returns all server settings, optionally filtered by category.
func (manager *Manager) ListSettings(ctx context.Context, req schema.SettingListRequest) (_ *schema.SettingList, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "ListSettings",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	var result schema.SettingList
	if err := manager.conn.List(ctx, &result, &req); err != nil {
		return nil, err
	}

	// Set the offset and limit in the result to reflect the actual count of items returned
	// which may be less than the requested limit if there are not enough items
	result.SettingListRequest = req
	result.OffsetLimit.Clamp(result.Count)

	// Return the result
	return &result, nil
}

// ListSettingCategories returns all distinct setting categories.
func (manager *Manager) ListSettingCategories(ctx context.Context, req schema.SettingCategoryListRequest) (_ *schema.SettingCategoryList, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "ListSettingCategories",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	var result schema.SettingCategoryList
	if err := manager.conn.List(ctx, &result, req); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetSetting returns a single setting by name.
func (manager *Manager) GetSetting(ctx context.Context, name string) (_ *schema.Setting, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "GetSetting",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	return getSetting(ctx, manager.conn, name)
}

// getSetting returns a setting, with its values in the configuration files,
// when they can be read
func getSetting(ctx context.Context, conn pg.Conn, name string) (*schema.Setting, error) {
	var result schema.Setting
	if err := conn.Get(ctx, &result, schema.SettingName(name)); err != nil {
		return nil, err
	}
	var access schema.SettingFileAccess
	if err := conn.Get(ctx, &access, access); err != nil {
		return nil, err
	} else if access {
		var files schema.SettingFiles
		if err := conn.List(ctx, &files, schema.SettingFileRequest(result.Name)); err != nil {
			return nil, err
		}
		result.Config, result.Override = files.Config, files.Override
	}
	return &result, nil
}

// UpdateSetting updates a setting value. If meta.Value is nil, the setting is reset to default.
// Returns the updated setting. Check the Context field to determine if Reload() or a
// server restart is needed for the change to take effect.
// Returns an error for settings with 'internal' context (cannot be changed) or
// 'postmaster' context (requires server restart, not supported via API).
func (manager *Manager) UpdateSetting(ctx context.Context, name string, meta schema.SettingMeta) (_ *schema.Setting, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "UpdateSetting",
		attribute.String("name", name),
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	// First get the current setting to check its context
	current, err := manager.GetSetting(ctx, name)
	if err != nil {
		return nil, err
	}

	// Reject updates for settings that cannot be changed dynamically
	switch current.Context {
	case "internal":
		return nil, pg.ErrBadParameter.Withf("setting %q cannot be changed (internal)", name)
	case "postmaster":
		return nil, pg.ErrBadParameter.Withf("setting %q requires server restart (postmaster context)", name)
	}

	// Update the setting (ALTER SYSTEM doesn't return rows, so pass nil reader)
	if err := manager.conn.Update(ctx, nil, schema.SettingName(name), meta); err != nil {
		return nil, err
	}

	// Reload, and return the setting, once it's reloaded
	var result *schema.Setting
	if err := reload(ctx, manager.conn, func(conn pg.Conn) (err error) {
		result, err = getSetting(ctx, conn, name)
		return err
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// Reload reloads the configuration files, and returns, once it's reloaded,
// when it was loaded, the settings which wait for the server to restart, and
// the errors in the configuration files, which pg_reload_conf() doesn't
// report, and whose settings are skipped. A setting which waits for a
// restart is an error in the files too, which isn't returned as one.
func (manager *Manager) Reload(ctx context.Context) (_ *schema.SettingReload, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "Reload")
	defer func() { endSpan(err) }()

	var result schema.SettingReload
	if err := reload(ctx, manager.conn, func(conn pg.Conn) error {
		var loaded schema.ConfigLoadTime
		if err := conn.Get(ctx, &loaded, loaded); err != nil {
			return err
		}
		result.Loaded = time.Time(loaded)

		var pending schema.SettingPendingRestart
		if err := conn.List(ctx, &pending, pending); err != nil {
			return err
		}
		result.PendingRestart = pending

		var access schema.SettingFileAccess
		if err := conn.Get(ctx, &access, access); err != nil || !access {
			return err
		}
		var errs schema.SettingFileErrors
		if err := conn.List(ctx, &errs, errs); err != nil {
			return err
		}
		for _, e := range errs {
			if !slices.ContainsFunc(pending, func(name string) bool { return strings.EqualFold(name, e.Name) }) {
				result.Errors = append(result.Errors, e)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &result, nil
}

// reload reloads the configuration files, which the server is signalled to
// do, and waits until a session's reloaded them, in which it calls the
// function, so what it reads is reloaded. A session reloads them before its
// next command, even in a transaction, which keeps the one session.
func reload(ctx context.Context, conn pg.Conn, fn func(pg.Conn) error) error {
	return conn.Tx(ctx, func(conn pg.Conn) error {
		var before schema.ConfigLoadTime
		if err := conn.Get(ctx, &before, before); err != nil {
			return err
		}
		if err := conn.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
			return err
		}
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			var after schema.ConfigLoadTime
			if err := conn.Get(ctx, &after, after); err != nil {
				return err
			}
			if time.Time(after).After(time.Time(before)) {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		}
		if fn != nil {
			return fn(conn)
		}
		return nil
	})
}
