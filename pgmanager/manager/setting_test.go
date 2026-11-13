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
