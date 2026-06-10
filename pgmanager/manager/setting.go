package manager

import (
	"context"

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

	var result schema.Setting
	if err := manager.conn.Get(ctx, &result, schema.SettingName(name)); err != nil {
		return nil, err
	}
	return &result, nil
}

// UpdateSetting updates a setting value. If meta.Value is nil, the setting is reset to default.
// Returns the updated setting. Check the Context field to determine if ReloadConfig() or a
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

	// Perform a reload
	if err := manager.ReloadConfig(ctx); err != nil {
		return nil, err
	}

	// Get and return the updated setting
	return manager.GetSetting(ctx, name)
}

// ReloadConfig calls pg_reload_conf() to reload server configuration.
// This applies changes to settings with 'sighup' context without requiring a restart.
func (manager *Manager) ReloadConfig(ctx context.Context) (err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "ReloadConfig")
	defer func() { endSpan(err) }()

	// Execute the reload command
	return manager.conn.Exec(ctx, "SELECT pg_reload_conf()")
}
