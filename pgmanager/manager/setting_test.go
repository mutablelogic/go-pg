package manager_test

import (
	"strings"
	"testing"

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
