package manager_test

import (
	"context"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pkg/manager"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	assert "github.com/stretchr/testify/assert"
)

////////////////////////////////////////////////////////////////////////////////
// LIST SETTINGS TESTS

func Test_Manager_ListSettings(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("ListAll", func(t *testing.T) {
		settings, err := mgr.ListSettings(context.TODO(), schema.SettingListRequest{})
		assert.NoError(err)
		assert.NotNil(settings)
		// PostgreSQL has many settings (typically 300+)
		assert.GreaterOrEqual(settings.Count, uint64(100))
		assert.NotEmpty(settings.Body)
	})

	t.Run("ListWithPagination", func(t *testing.T) {
		limit := uint64(10)
		settings, err := mgr.ListSettings(context.TODO(), schema.SettingListRequest{
			OffsetLimit: pg.OffsetLimit{Limit: &limit},
		})
		assert.NoError(err)
		assert.NotNil(settings)
		assert.LessOrEqual(len(settings.Body), 10)
	})

	t.Run("ListWithOffset", func(t *testing.T) {
		// First get all settings
		allSettings, err := mgr.ListSettings(context.TODO(), schema.SettingListRequest{})
		assert.NoError(err)

		// Get with offset
		limit := uint64(500)
		settings, err := mgr.ListSettings(context.TODO(), schema.SettingListRequest{
			OffsetLimit: pg.OffsetLimit{Offset: 10, Limit: &limit},
		})
		assert.NoError(err)
		assert.NotNil(settings)
		assert.Less(len(settings.Body), int(allSettings.Count))
	})

	t.Run("ListByCategory", func(t *testing.T) {
		// First list all to find a valid category
		allSettings, err := mgr.ListSettings(context.TODO(), schema.SettingListRequest{})
		assert.NoError(err)
		assert.NotEmpty(allSettings.Body)

		// Use the category from the first setting
		category := allSettings.Body[0].Category
		settings, err := mgr.ListSettings(context.TODO(), schema.SettingListRequest{
			Category: &category,
		})
		assert.NoError(err)
		assert.NotNil(settings)
		assert.NotEmpty(settings.Body)
		// All returned settings should be in the specified category
		for _, s := range settings.Body {
			assert.Equal(category, s.Category)
		}
	})

	t.Run("ListByCategoryNotFound", func(t *testing.T) {
		category := "NonExistent Category XYZ"
		settings, err := mgr.ListSettings(context.TODO(), schema.SettingListRequest{
			Category: &category,
		})
		assert.NoError(err)
		assert.NotNil(settings)
		assert.Empty(settings.Body)
		assert.Equal(uint64(0), settings.Count)
	})

	t.Run("SettingHasRequiredFields", func(t *testing.T) {
		limit := uint64(5)
		settings, err := mgr.ListSettings(context.TODO(), schema.SettingListRequest{
			OffsetLimit: pg.OffsetLimit{Limit: &limit},
		})
		assert.NoError(err)
		assert.NotEmpty(settings.Body)

		for _, s := range settings.Body {
			assert.NotEmpty(s.Name, "Setting should have a name")
			assert.NotEmpty(s.Category, "Setting should have a category")
			assert.NotEmpty(s.Context, "Setting should have a context")
		}
	})

	t.Run("SettingContextValues", func(t *testing.T) {
		settings, err := mgr.ListSettings(context.TODO(), schema.SettingListRequest{})
		assert.NoError(err)
		assert.NotEmpty(settings.Body)

		validContexts := map[string]bool{
			"internal":          true,
			"postmaster":        true,
			"sighup":            true,
			"superuser":         true,
			"user":              true,
			"backend":           true,
			"superuser-backend": true,
		}

		for _, s := range settings.Body {
			assert.True(validContexts[s.Context], "Setting %s has invalid context: %s", s.Name, s.Context)
		}
	})

	t.Run("KnownSettingsExist", func(t *testing.T) {
		settings, err := mgr.ListSettings(context.TODO(), schema.SettingListRequest{})
		assert.NoError(err)

		// Build a map for easy lookup
		settingMap := make(map[string]schema.Setting)
		for _, s := range settings.Body {
			settingMap[s.Name] = s
		}

		// Check some well-known settings exist
		knownSettings := []string{"max_connections", "shared_buffers", "work_mem", "server_version"}
		for _, name := range knownSettings {
			_, exists := settingMap[name]
			assert.True(exists, "Expected setting %s to exist", name)
		}
	})
}

////////////////////////////////////////////////////////////////////////////////
// LIST SETTING CATEGORIES TESTS

func Test_Manager_ListSettingCategories(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("ListAll", func(t *testing.T) {
		categories, err := mgr.ListSettingCategories(context.TODO())
		assert.NoError(err)
		assert.NotNil(categories)
		// PostgreSQL has multiple setting categories
		assert.GreaterOrEqual(categories.Count, uint64(10))
		assert.NotEmpty(categories.Body)
	})

	t.Run("CategoriesAreSorted", func(t *testing.T) {
		categories, err := mgr.ListSettingCategories(context.TODO())
		assert.NoError(err)
		assert.NotEmpty(categories.Body)

		// Check categories are sorted alphabetically
		for i := 1; i < len(categories.Body); i++ {
			assert.LessOrEqual(categories.Body[i-1], categories.Body[i], "Categories should be sorted")
		}
	})

	t.Run("CategoriesAreUnique", func(t *testing.T) {
		categories, err := mgr.ListSettingCategories(context.TODO())
		assert.NoError(err)

		// Check all categories are unique
		seen := make(map[string]bool)
		for _, cat := range categories.Body {
			assert.False(seen[cat], "Category %s should be unique", cat)
			seen[cat] = true
		}
	})

	t.Run("KnownCategoriesExist", func(t *testing.T) {
		categories, err := mgr.ListSettingCategories(context.TODO())
		assert.NoError(err)
		assert.NotEmpty(categories.Body)

		// Verify first category can be used to filter settings
		firstCategory := categories.Body[0]
		settings, err := mgr.ListSettings(context.TODO(), schema.SettingListRequest{
			Category: &firstCategory,
		})
		assert.NoError(err)
		assert.NotEmpty(settings.Body)
		// All settings should match the category
		for _, s := range settings.Body {
			assert.Equal(firstCategory, s.Category)
		}
	})

	t.Run("CountMatchesBody", func(t *testing.T) {
		categories, err := mgr.ListSettingCategories(context.TODO())
		assert.NoError(err)
		assert.Equal(int(categories.Count), len(categories.Body))
	})
}

////////////////////////////////////////////////////////////////////////////////
// GET SETTING TESTS

func Test_Manager_GetSetting(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("GetExisting", func(t *testing.T) {
		setting, err := mgr.GetSetting(context.TODO(), "max_connections")
		assert.NoError(err)
		assert.NotNil(setting)
		assert.Equal("max_connections", setting.Name)
		assert.NotNil(setting.Value)
		assert.NotEmpty(setting.Category)
		assert.NotEmpty(setting.Context)
	})

	t.Run("GetNotFound", func(t *testing.T) {
		setting, err := mgr.GetSetting(context.TODO(), "nonexistent_setting_xyz")
		assert.Error(err)
		assert.Nil(setting)
	})

	t.Run("GetEmptyName", func(t *testing.T) {
		setting, err := mgr.GetSetting(context.TODO(), "")
		assert.Error(err)
		assert.Nil(setting)
	})

	t.Run("GetSettingWithUnit", func(t *testing.T) {
		// shared_buffers typically has a unit (kB, MB, etc.)
		setting, err := mgr.GetSetting(context.TODO(), "shared_buffers")
		assert.NoError(err)
		assert.NotNil(setting)
		assert.Equal("shared_buffers", setting.Name)
		// shared_buffers should have a unit like "8kB"
		assert.NotNil(setting.Unit)
	})
}

////////////////////////////////////////////////////////////////////////////////
// UPDATE SETTING TESTS

func Test_Manager_UpdateSetting(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("UpdateSuperuserSetting", func(t *testing.T) {
		// log_min_duration_statement has superuser context
		newValue := "500"
		setting, err := mgr.UpdateSetting(context.TODO(), "log_min_duration_statement", schema.SettingMeta{
			Value: &newValue,
		})
		assert.NoError(err)
		assert.NotNil(setting)
		assert.Equal("log_min_duration_statement", setting.Name)
	})

	t.Run("ResetSetting", func(t *testing.T) {
		// Reset by passing nil value
		setting, err := mgr.UpdateSetting(context.TODO(), "log_min_duration_statement", schema.SettingMeta{
			Value: nil,
		})
		assert.NoError(err)
		assert.NotNil(setting)
		assert.Equal("log_min_duration_statement", setting.Name)
	})

	t.Run("UpdateNotFound", func(t *testing.T) {
		newValue := "100"
		setting, err := mgr.UpdateSetting(context.TODO(), "nonexistent_setting_xyz", schema.SettingMeta{
			Value: &newValue,
		})
		assert.Error(err)
		assert.Nil(setting)
	})

	t.Run("RejectInternalContext", func(t *testing.T) {
		// block_size is an internal setting (cannot be changed)
		newValue := "16384"
		setting, err := mgr.UpdateSetting(context.TODO(), "block_size", schema.SettingMeta{
			Value: &newValue,
		})
		assert.Error(err)
		assert.Nil(setting)
		assert.Contains(err.Error(), "internal")
	})

	t.Run("RejectPostmasterContext", func(t *testing.T) {
		// max_connections is a postmaster setting (requires restart)
		newValue := "200"
		setting, err := mgr.UpdateSetting(context.TODO(), "max_connections", schema.SettingMeta{
			Value: &newValue,
		})
		assert.Error(err)
		assert.Nil(setting)
		assert.Contains(err.Error(), "postmaster")
	})
}

////////////////////////////////////////////////////////////////////////////////
// RELOAD CONFIG TESTS

func Test_Manager_ReloadConfig(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("ReloadSuccess", func(t *testing.T) {
		err := mgr.ReloadConfig(context.TODO())
		assert.NoError(err)
	})
}
