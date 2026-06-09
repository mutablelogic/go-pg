package manager

import (
	"context"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListSettings returns all server settings, optionally filtered by category.
func (manager *Manager) ListSettings(ctx context.Context, req schema.SettingListRequest) (*schema.SettingList, error) {
	var list schema.SettingList
	if err := manager.conn.List(ctx, &list, req); err != nil {
		return nil, err
	}
	return &list, nil
}

// ListSettingCategories returns all distinct setting categories.
func (manager *Manager) ListSettingCategories(ctx context.Context) (*schema.SettingCategoryList, error) {
	var list schema.SettingCategoryList
	if err := manager.conn.List(ctx, &list, schema.SettingCategoryListRequest{}); err != nil {
		return nil, err
	}
	return &list, nil
}

// GetSetting returns a single setting by name.
func (manager *Manager) GetSetting(ctx context.Context, name string) (*schema.Setting, error) {
	var setting schema.Setting
	if err := manager.conn.Get(ctx, &setting, schema.SettingName(name)); err != nil {
		return nil, err
	}
	return &setting, nil
}

// UpdateSetting updates a setting value. If meta.Value is nil, the setting is reset to default.
// Returns the updated setting. Check the Context field to determine if ReloadConfig() or a
// server restart is needed for the change to take effect.
// Returns an error for settings with 'internal' context (cannot be changed) or
// 'postmaster' context (requires server restart, not supported via API).
func (manager *Manager) UpdateSetting(ctx context.Context, name string, meta schema.SettingMeta) (*schema.Setting, error) {
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

	// Get and return the updated setting
	return manager.GetSetting(ctx, name)
}

// ReloadConfig calls pg_reload_conf() to reload server configuration.
// This applies changes to settings with 'sighup' context without requiring a restart.
func (manager *Manager) ReloadConfig(ctx context.Context) error {
	return manager.conn.Exec(ctx, "SELECT pg_reload_conf()")
}
