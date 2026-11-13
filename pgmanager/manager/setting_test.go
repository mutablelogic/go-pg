package manager_test

import (
	"context"
	"strings"
	"testing"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	harness "github.com/mutablelogic/go-pg/pgmanager/test"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// The categories are the top levels, whose settings are those listed by
// them, and which have every setting between them
func TestSettingCategories(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	categories, err := mgr.ListSettingCategories(ctx, schema.SettingCategoryListRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, categories.Body)
	assert.Equal(t, uint64(len(categories.Body)), categories.Count)

	all, err := mgr.ListSettings(ctx, schema.SettingListRequest{})
	require.NoError(t, err)
	var total, changed uint64
	for _, category := range categories.Body {
		assert.NotContains(t, category.Name, " / ")
		total += category.Count
		changed += category.Changed

		// The settings of a top level are its own, and those under it
		list, err := mgr.ListSettings(ctx, schema.SettingListRequest{Category: types.Ptr(category.Name)})
		require.NoError(t, err)
		assert.Equal(t, category.Count, list.Count, category.Name)
		for _, setting := range list.Body {
			assert.True(t, setting.Category == category.Name || strings.HasPrefix(setting.Category, category.Name+" / "), setting.Category)
		}
	}
	assert.Equal(t, all.Count, total)
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
		assert.False(t, setting.PendingRestart, test.name)
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

	require.NoError(t, harness.Pool().Exec(ctx, `ALTER SYSTEM SET max_connections = 101`))
	defer func() {
		_ = harness.Pool().Exec(context.Background(), `ALTER SYSTEM RESET max_connections`)
		_ = harness.Pool().Exec(context.Background(), `SELECT pg_reload_conf()`)
	}()
	require.NoError(t, harness.Pool().Exec(ctx, `SELECT pg_reload_conf()`))

	// The reload is signalled, so it's waited for
	var setting *schema.Setting
	for range 50 {
		var err error
		setting, err = mgr.GetSetting(ctx, "max_connections")
		require.NoError(t, err)
		if setting.PendingRestart {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.True(t, setting.PendingRestart)
	assert.Equal(t, "100", types.Value(setting.Value))
}
