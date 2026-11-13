package manager_test

import (
	"context"
	"strings"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pgmanager/manager"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	harness "github.com/mutablelogic/go-pg/pgmanager/test"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// allSettings returns every setting of a category, a page at a time
func allSettings(t *testing.T, ctx context.Context, mgr *manager.Manager, category *string) []schema.Setting {
	t.Helper()
	var settings []schema.Setting
	for {
		list, err := mgr.ListSettings(ctx, schema.SettingListRequest{OffsetLimit: pg.OffsetLimit{Offset: uint64(len(settings))}, Category: category})
		require.NoError(t, err)
		settings = append(settings, list.Body...)
		if len(list.Body) == 0 || uint64(len(settings)) >= list.Count {
			require.Equal(t, list.Count, uint64(len(settings)))
			return settings
		}
	}
}

// The categories are the top levels, whose settings are those listed by
// them, and which have every setting between them, and as many changed
func TestSettingCategories(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	categories, err := mgr.ListSettingCategories(ctx, schema.SettingCategoryListRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, categories.Body)
	assert.Equal(t, uint64(len(categories.Body)), categories.Count)

	all := allSettings(t, ctx, mgr, nil)
	var total, changed uint64
	for _, category := range categories.Body {
		assert.NotContains(t, category.Name, " / ")
		total += category.Count
		changed += category.Changed

		// The settings of a top level are its own, and those under it, of
		// which as many are changed
		settings := allSettings(t, ctx, mgr, types.Ptr(category.Name))
		assert.Equal(t, category.Count, uint64(len(settings)), category.Name)
		var n uint64
		for _, setting := range settings {
			assert.True(t, setting.Category == category.Name || strings.HasPrefix(setting.Category, category.Name+" / "), setting.Category)
			if setting.Changed {
				n++
			}
		}
		assert.Equal(t, category.Changed, n, category.Name)
	}

	// Between them, they've every setting, and every one which is changed
	var n uint64
	for _, setting := range all {
		if setting.Changed {
			n++
		}
	}
	assert.Equal(t, uint64(len(all)), total)
	assert.Equal(t, n, changed)
	assert.NotZero(t, changed)

	// A category of two levels is only its own
	list, err := mgr.ListSettings(ctx, schema.SettingListRequest{Category: types.Ptr("Client Connection Defaults / Locale and Formatting")})
	require.NoError(t, err)
	assert.NotZero(t, list.Count)
	for _, setting := range list.Body {
		assert.Equal(t, "Client Connection Defaults / Locale and Formatting", setting.Category)
	}
}

// A setting has its type, its limits, or its values, and its default
func TestSettingFields(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	tests := []struct {
		name, kind string
		limits     bool
		values     []string
	}{
		{"autovacuum", "bool", false, nil},
		{"max_connections", "integer", true, nil},
		{"autovacuum_vacuum_scale_factor", "real", true, nil},
		{"wal_level", "enum", false, []string{"minimal", "replica", "logical"}},
		{"application_name", "string", false, nil},
	}
	for _, test := range tests {
		setting, err := mgr.GetSetting(ctx, test.name)
		require.NoError(t, err)
		assert.Equal(t, test.kind, setting.Type, test.name)
		assert.Equal(t, test.limits, setting.Min != nil && setting.Max != nil, test.name)
		assert.Equal(t, test.values, setting.Values, test.name)
		assert.NotEmpty(t, setting.Source, test.name)
	}

	// max_connections is built with a default of 100
	setting, err := mgr.GetSetting(ctx, "max_connections")
	require.NoError(t, err)
	assert.Equal(t, "100", types.Value(setting.Default))
}

// A setting which needs a restart, which is changed, and reloaded, is
// pending its restart
func TestSettingPendingRestart(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	// The reload is signalled, so it's waited for, until max_connections is
	// pending, or isn't
	pending := func(want bool) *schema.Setting {
		t.Helper()
		var setting *schema.Setting
		for range 50 {
			var err error
			setting, err = mgr.GetSetting(context.Background(), "max_connections")
			require.NoError(t, err)
			if setting.PendingRestart == want {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		return setting
	}

	require.NoError(t, harness.Pool().Exec(ctx, `ALTER SYSTEM SET max_connections = 101`))
	defer func() {
		// It's reset, and reloaded, before the next test, which shares the
		// server
		require.NoError(t, harness.Pool().Exec(context.Background(), `ALTER SYSTEM RESET max_connections`))
		require.NoError(t, harness.Pool().Exec(context.Background(), `SELECT pg_reload_conf()`))
		assert.False(t, pending(false).PendingRestart)
	}()
	require.NoError(t, harness.Pool().Exec(ctx, `SELECT pg_reload_conf()`))

	setting := pending(true)
	assert.True(t, setting.PendingRestart)
	assert.Equal(t, "100", types.Value(setting.Value))
}

// A setting which is changed is overridden, in postgresql.auto.conf, and
// takes effect once it's returned, and one which is reset isn't
func TestSettingChangeAndReset(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)
	defer func() { _, _ = mgr.UpdateSetting(context.Background(), "work_mem", schema.SettingMeta{}) }()

	// Changed, which is in kB, and as it's written
	setting, err := mgr.UpdateSetting(ctx, "work_mem", schema.SettingMeta{Value: types.Ptr("8MB")})
	require.NoError(t, err)
	assert.Equal(t, "8192", types.Value(setting.Value))
	assert.Equal(t, "8MB", types.Value(setting.Override))
	assert.True(t, setting.Changed)

	// It's read the same, by its name
	setting, err = mgr.GetSetting(ctx, "work_mem")
	require.NoError(t, err)
	assert.Equal(t, "8192", types.Value(setting.Value))
	assert.Equal(t, "8MB", types.Value(setting.Override))

	// Reset, which isn't overridden, and is its default, as the
	// configuration files don't set it
	setting, err = mgr.UpdateSetting(ctx, "work_mem", schema.SettingMeta{})
	require.NoError(t, err)
	assert.Nil(t, setting.Override)
	assert.Equal(t, types.Value(setting.Default), types.Value(setting.Value))

	// A setting which needs a restart, or can't be changed, isn't changed
	_, err = mgr.UpdateSetting(ctx, "max_connections", schema.SettingMeta{Value: types.Ptr("120")})
	assert.Error(t, err)
	_, err = mgr.UpdateSetting(ctx, "block_size", schema.SettingMeta{Value: types.Ptr("16384")})
	assert.Error(t, err)
}

// A setting in postgresql.conf has its value there, and none overriding it
func TestSettingConfig(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	setting, err := mgr.GetSetting(ctx, "listen_addresses")
	require.NoError(t, err)
	assert.Equal(t, "*", types.Value(setting.Config))
	assert.Nil(t, setting.Override)
}

// Reloading returns when it was loaded, and a setting which waits for a
// restart, which isn't an error
func TestSettingReload(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	before, err := mgr.Reload(ctx)
	require.NoError(t, err)
	assert.False(t, before.Loaded.IsZero())
	assert.Empty(t, before.Errors)

	require.NoError(t, harness.Pool().Exec(ctx, `ALTER SYSTEM SET max_connections = 101`))
	defer func() {
		require.NoError(t, harness.Pool().Exec(context.Background(), `ALTER SYSTEM RESET max_connections`))
		after, err := mgr.Reload(context.Background())
		require.NoError(t, err)
		assert.NotContains(t, after.PendingRestart, "max_connections")
	}()

	// Waited for, so it's pending as soon as it's returned, and later than
	// it was loaded before
	result, err := mgr.Reload(ctx)
	require.NoError(t, err)
	assert.True(t, result.Loaded.After(before.Loaded))
	assert.Contains(t, result.PendingRestart, "max_connections")
	for _, e := range result.Errors {
		assert.NotEqual(t, "max_connections", e.Name)
	}
}

// Restarting isn't available, unless it's turned on, and the categories say
// so, and what waits for a restart
func TestSettingRestartNotAvailable(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	_, err := mgr.Restart(ctx)
	assert.ErrorIs(t, err, pg.ErrNotAvailable)

	require.NoError(t, harness.Pool().Exec(ctx, `ALTER SYSTEM SET max_connections = 101`))
	defer func() {
		require.NoError(t, harness.Pool().Exec(context.Background(), `ALTER SYSTEM RESET max_connections`))
		_, err := mgr.Reload(context.Background())
		require.NoError(t, err)
	}()
	_, err = mgr.Reload(ctx)
	require.NoError(t, err)

	categories, err := mgr.ListSettingCategories(ctx, schema.SettingCategoryListRequest{})
	require.NoError(t, err)
	assert.False(t, categories.Restart)
	assert.Contains(t, categories.PendingRestart, "max_connections")
}
