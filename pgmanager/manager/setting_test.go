package manager_test

import (
	"context"
	"strings"
	"testing"

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
