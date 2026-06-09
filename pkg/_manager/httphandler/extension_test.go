package httphandler_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	// Packages
	httphandler "github.com/mutablelogic/go-pg/pkg/manager/httphandler"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	test "github.com/mutablelogic/go-pg/pkg/test"
	assert "github.com/stretchr/testify/assert"
)

///////////////////////////////////////////////////////////////////////////////
// TESTS

func Test_Extension_RegisterHandlers(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	t.Run("PanicOnNilManager", func(t *testing.T) {
		router := http.NewServeMux()
		assert.Panics(func() {
			httphandler.RegisterExtensionHandlers(router, "/api", nil)
		})
	})

	t.Run("RegisterSuccess", func(t *testing.T) {
		router := http.NewServeMux()
		assert.NotPanics(func() {
			httphandler.RegisterExtensionHandlers(router, "/api", manager.Manager)
		})
	})
}

func Test_Extension_List(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httphandler.RegisterExtensionHandlers(router, "/api", manager.Manager)

	t.Run("ListExtensions", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/extension", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.ExtensionList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.GreaterOrEqual(int(resp.Count), 1) // At least some available extensions
	})

	t.Run("ListExtensionsWithPagination", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/extension?limit=1", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		var resp schema.ExtensionList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.LessOrEqual(len(resp.Body), 1)
	})

	t.Run("ListInstalledOnly", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/extension?installed=true", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		var resp schema.ExtensionList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		// All returned extensions should have InstalledVersion set
		for _, ext := range resp.Body {
			assert.NotNil(ext.InstalledVersion, "Extension %s should have InstalledVersion", ext.Name)
		}
	})

	t.Run("ListNotInstalledOnly", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/extension?installed=false", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		var resp schema.ExtensionList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		// All returned extensions should NOT have InstalledVersion set
		for _, ext := range resp.Body {
			assert.Nil(ext.InstalledVersion, "Extension %s should not have InstalledVersion", ext.Name)
		}
	})

	t.Run("ListByDatabase", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/extension?database=postgres", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		var resp schema.ExtensionList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		// All returned extensions should be from the postgres database
		for _, ext := range resp.Body {
			assert.Equal("postgres", ext.Database)
		}
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/extension", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_Extension_Get(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httphandler.RegisterExtensionHandlers(router, "/api", manager.Manager)

	t.Run("GetExisting", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/extension/plpgsql", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		// Parse response
		var ext schema.Extension
		err := json.Unmarshal(w.Body.Bytes(), &ext)
		assert.NoError(err)
		assert.Equal("plpgsql", ext.Name)
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/extension/plpgsql", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_Extension_Create(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httphandler.RegisterExtensionHandlers(router, "/api", manager.Manager)

	t.Run("CreateMissingDatabase", func(t *testing.T) {
		body := `{"name": "hstore"}`
		req := httptest.NewRequest(http.MethodPost, "/api/extension", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		// Create requires database field
		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateEmptyName", func(t *testing.T) {
		body := `{"name": "", "database": "postgres"}`
		req := httptest.NewRequest(http.MethodPost, "/api/extension", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		// Returns bad request for empty name
		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateWithSchemaMissingDatabase", func(t *testing.T) {
		body := `{"name": "hstore", "schema": "public"}`
		req := httptest.NewRequest(http.MethodPost, "/api/extension", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		// Create requires database field
		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateWithVersionMissingDatabase", func(t *testing.T) {
		body := `{"name": "hstore", "version": "1.8"}`
		req := httptest.NewRequest(http.MethodPost, "/api/extension", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		// Create requires database field
		assert.Equal(http.StatusBadRequest, w.Code)
	})
}

func Test_Extension_Update(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httphandler.RegisterExtensionHandlers(router, "/api", manager.Manager)

	t.Run("UpdateMissingDatabase", func(t *testing.T) {
		body := `{"version": "1.0"}`
		req := httptest.NewRequest(http.MethodPatch, "/api/extension/plpgsql", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		// Update requires database field
		assert.Equal(http.StatusBadRequest, w.Code)
	})
}

func Test_Extension_Delete(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httphandler.RegisterExtensionHandlers(router, "/api", manager.Manager)

	t.Run("DeleteMissingDatabase", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/extension/hstore", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		// Delete requires database query parameter
		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("DeleteWithForceMissingDatabase", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/extension/hstore?force=true", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		// Delete requires database query parameter
		assert.Equal(http.StatusBadRequest, w.Code)
	})
}
