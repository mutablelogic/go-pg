package httphandler_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	// Packages
	httprequest "github.com/mutablelogic/go-pg/pkg/manager/httphandler"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	test "github.com/mutablelogic/go-pg/pkg/test"
	assert "github.com/stretchr/testify/assert"
)

///////////////////////////////////////////////////////////////////////////////
// TESTS

func Test_Database_RegisterHandlers(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	t.Run("PanicOnNilManager", func(t *testing.T) {
		router := http.NewServeMux()
		assert.Panics(func() {
			httprequest.RegisterDatabaseHandlers(router, "/api", nil)
		})
	})

	t.Run("RegisterSuccess", func(t *testing.T) {
		router := http.NewServeMux()
		assert.NotPanics(func() {
			httprequest.RegisterDatabaseHandlers(router, "/api", manager.Manager)
		})
	})
}

func Test_Database_List(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterDatabaseHandlers(router, "/api", manager.Manager)

	t.Run("ListDatabases", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/database", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.DatabaseList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.GreaterOrEqual(int(resp.Count), 1) // At least the default database
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/database", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_Database_Get(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterDatabaseHandlers(router, "/api", manager.Manager)

	t.Run("GetExisting", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/database/postgres", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.Database
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal("postgres", resp.Name)
	})

	t.Run("GetNotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/database/nonexistent_db_xyz", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("GetReservedPrefix", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/database/pg_reserved", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/database/postgres", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_Database_Create(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterDatabaseHandlers(router, "/api", manager.Manager)

	t.Run("CreateSuccess", func(t *testing.T) {
		body := `{"name": "test_http_create"}`
		req := httptest.NewRequest(http.MethodPost, "/api/database", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusCreated, w.Code)

		var resp schema.Database
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal("test_http_create", resp.Name)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteDatabase(req.Context(), "test_http_create", true)
		})
	})

	t.Run("CreateEmptyName", func(t *testing.T) {
		body := `{"name": ""}`
		req := httptest.NewRequest(http.MethodPost, "/api/database", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateReservedPrefix", func(t *testing.T) {
		body := `{"name": "pg_reserved_test"}`
		req := httptest.NewRequest(http.MethodPost, "/api/database", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateInvalidJSON", func(t *testing.T) {
		body := `{invalid json}`
		req := httptest.NewRequest(http.MethodPost, "/api/database", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		// First create
		body := `{"name": "test_http_duplicate"}`
		req := httptest.NewRequest(http.MethodPost, "/api/database", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)
		assert.Equal(http.StatusCreated, w.Code)

		// Second create should fail
		req2 := httptest.NewRequest(http.MethodPost, "/api/database", bytes.NewBufferString(body))
		req2.Header.Set("Content-Type", "application/json")
		w2 := httptest.NewRecorder()

		router.ServeHTTP(w2, req2)
		// Duplicate database returns 500 (PostgreSQL error)
		assert.Equal(http.StatusInternalServerError, w2.Code)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteDatabase(req.Context(), "test_http_duplicate", true)
		})
	})
}

func Test_Database_Delete(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterDatabaseHandlers(router, "/api", manager.Manager)

	t.Run("DeleteExisting", func(t *testing.T) {
		// First create a database
		body := `{"name": "test_http_delete"}`
		createReq := httptest.NewRequest(http.MethodPost, "/api/database", bytes.NewBufferString(body))
		createReq.Header.Set("Content-Type", "application/json")
		createW := httptest.NewRecorder()
		router.ServeHTTP(createW, createReq)
		assert.Equal(http.StatusCreated, createW.Code)

		// Delete the database
		req := httptest.NewRequest(http.MethodDelete, "/api/database/test_http_delete", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
	})

	t.Run("DeleteWithForce", func(t *testing.T) {
		// First create a database
		body := `{"name": "test_http_delete_force"}`
		createReq := httptest.NewRequest(http.MethodPost, "/api/database", bytes.NewBufferString(body))
		createReq.Header.Set("Content-Type", "application/json")
		createW := httptest.NewRecorder()
		router.ServeHTTP(createW, createReq)
		assert.Equal(http.StatusCreated, createW.Code)

		// Delete with force
		req := httptest.NewRequest(http.MethodDelete, "/api/database/test_http_delete_force?force=true", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/database/nonexistent_db_xyz", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("DeleteReservedPrefix", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/database/pg_reserved", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})
}

func Test_Database_Update(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterDatabaseHandlers(router, "/api", manager.Manager)

	t.Run("UpdateRename", func(t *testing.T) {
		// First create a database
		body := `{"name": "test_http_update_old"}`
		createReq := httptest.NewRequest(http.MethodPost, "/api/database", bytes.NewBufferString(body))
		createReq.Header.Set("Content-Type", "application/json")
		createW := httptest.NewRecorder()
		router.ServeHTTP(createW, createReq)
		assert.Equal(http.StatusCreated, createW.Code)

		// Update (rename) the database
		updateBody := `{"name": "test_http_update_new"}`
		req := httptest.NewRequest(http.MethodPatch, "/api/database/test_http_update_old", bytes.NewBufferString(updateBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		var resp schema.Database
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal("test_http_update_new", resp.Name)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteDatabase(req.Context(), "test_http_update_new", true)
		})
	})

	t.Run("UpdateNotFound", func(t *testing.T) {
		body := `{"name": "new_name"}`
		req := httptest.NewRequest(http.MethodPatch, "/api/database/nonexistent_db_xyz", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("UpdateToReservedPrefix", func(t *testing.T) {
		// First create a database
		body := `{"name": "test_http_update_reserved"}`
		createReq := httptest.NewRequest(http.MethodPost, "/api/database", bytes.NewBufferString(body))
		createReq.Header.Set("Content-Type", "application/json")
		createW := httptest.NewRecorder()
		router.ServeHTTP(createW, createReq)
		assert.Equal(http.StatusCreated, createW.Code)

		// Try to rename to reserved prefix
		updateBody := `{"name": "pg_reserved_name"}`
		req := httptest.NewRequest(http.MethodPatch, "/api/database/test_http_update_reserved", bytes.NewBufferString(updateBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteDatabase(req.Context(), "test_http_update_reserved", true)
		})
	})

	t.Run("UpdateReservedPrefixSource", func(t *testing.T) {
		body := `{"name": "new_name"}`
		req := httptest.NewRequest(http.MethodPatch, "/api/database/pg_reserved", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("UpdateInvalidJSON", func(t *testing.T) {
		body := `{invalid json}`
		req := httptest.NewRequest(http.MethodPatch, "/api/database/postgres", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})
}

func Test_httperr(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterDatabaseHandlers(router, "/api", manager.Manager)

	// Test error mapping through actual HTTP calls
	t.Run("NotFoundMapsTo404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/database/this_db_does_not_exist_xyz", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("BadParameterMapsTo400", func(t *testing.T) {
		// Empty name in create
		body := `{"name": ""}`
		req := httptest.NewRequest(http.MethodPost, "/api/database", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})
}
