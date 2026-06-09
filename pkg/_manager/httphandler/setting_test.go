package httphandler_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	// Packages
	httphandler "github.com/mutablelogic/go-pg/pkg/manager/httphandler"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	test "github.com/mutablelogic/go-pg/pkg/test"
	assert "github.com/stretchr/testify/assert"
)

///////////////////////////////////////////////////////////////////////////////
// TESTS

func Test_Setting_RegisterHandlers(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	t.Run("PanicOnNilManager", func(t *testing.T) {
		router := http.NewServeMux()
		assert.Panics(func() {
			httphandler.RegisterSettingHandlers(router, "/api", nil)
		})
	})

	t.Run("RegisterSuccess", func(t *testing.T) {
		router := http.NewServeMux()
		assert.NotPanics(func() {
			httphandler.RegisterSettingHandlers(router, "/api", manager.Manager)
		})
	})
}

func Test_Setting_List(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httphandler.RegisterSettingHandlers(router, "/api", manager.Manager)

	t.Run("ListSettings", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/setting", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.SettingList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Greater(int(resp.Count), 100) // PostgreSQL has 300+ settings
	})

	t.Run("ListSettingsWithCategory", func(t *testing.T) {
		// First get a valid category
		req := httptest.NewRequest(http.MethodGet, "/api/setting/category", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		var categories schema.SettingCategoryList
		err := json.Unmarshal(w.Body.Bytes(), &categories)
		assert.NoError(err)
		assert.Greater(len(categories.Body), 0)

		// Use first category to filter (URL-encode the category name)
		category := categories.Body[0]
		req = httptest.NewRequest(http.MethodGet, "/api/setting?category="+url.QueryEscape(category), nil)
		w = httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		var resp schema.SettingList
		err = json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Greater(int(resp.Count), 0)

		// All settings should be in the requested category
		for _, s := range resp.Body {
			assert.Equal(category, s.Category)
		}
	})

	t.Run("ListSettingsWithPagination", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/setting?limit=10", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		var resp schema.SettingList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.LessOrEqual(len(resp.Body), 10)
		assert.Greater(int(resp.Count), 10) // Total count should be > limit
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/setting", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})

	t.Run("PostNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/setting", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_Setting_CategoryList(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httphandler.RegisterSettingHandlers(router, "/api", manager.Manager)

	t.Run("ListCategories", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/setting/category", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.SettingCategoryList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Greater(int(resp.Count), 10) // Should have 10+ categories
		assert.Equal(int(resp.Count), len(resp.Body))
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/setting/category", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_Setting_Get(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httphandler.RegisterSettingHandlers(router, "/api", manager.Manager)

	t.Run("GetExisting", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/setting/max_connections", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		// Parse response
		var setting schema.Setting
		err := json.Unmarshal(w.Body.Bytes(), &setting)
		assert.NoError(err)
		assert.Equal("max_connections", setting.Name)
		assert.NotNil(setting.Value)
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/setting/max_connections", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_Setting_Update(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httphandler.RegisterSettingHandlers(router, "/api", manager.Manager)

	t.Run("UpdateSetting", func(t *testing.T) {
		body := map[string]interface{}{
			"value": "120",
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPatch, "/api/setting/log_min_duration_statement", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		// Parse response
		var setting schema.Setting
		err := json.Unmarshal(w.Body.Bytes(), &setting)
		assert.NoError(err)
		assert.Equal("log_min_duration_statement", setting.Name)
	})

	t.Run("ResetSetting", func(t *testing.T) {
		// value: null means reset
		body := map[string]interface{}{
			"value": nil,
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPatch, "/api/setting/log_min_duration_statement", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		// Parse response
		var setting schema.Setting
		err := json.Unmarshal(w.Body.Bytes(), &setting)
		assert.NoError(err)
		assert.Equal("log_min_duration_statement", setting.Name)
	})

	t.Run("UpdateWithReload", func(t *testing.T) {
		body := map[string]interface{}{
			"value": "120",
		}
		bodyBytes, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPatch, "/api/setting/log_min_duration_statement?reload=true", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		// Parse response
		var setting schema.Setting
		err := json.Unmarshal(w.Body.Bytes(), &setting)
		assert.NoError(err)
		assert.Equal("log_min_duration_statement", setting.Name)
	})
}
