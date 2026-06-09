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

func Test_Role_RegisterHandlers(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	t.Run("PanicOnNilManager", func(t *testing.T) {
		router := http.NewServeMux()
		assert.Panics(func() {
			httprequest.RegisterRoleHandlers(router, "/api", nil)
		})
	})

	t.Run("RegisterSuccess", func(t *testing.T) {
		router := http.NewServeMux()
		assert.NotPanics(func() {
			httprequest.RegisterRoleHandlers(router, "/api", manager.Manager)
		})
	})
}

func Test_Role_List(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterRoleHandlers(router, "/api", manager.Manager)

	t.Run("ListRoles", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/role", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.RoleList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.GreaterOrEqual(int(resp.Count), 1) // At least the default postgres role
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/role", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_Role_Get(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterRoleHandlers(router, "/api", manager.Manager)

	t.Run("GetExisting", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/role/postgres", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.Role
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal("postgres", resp.Name)
	})

	t.Run("GetNotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/role/nonexistent_role_xyz", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("GetReservedPrefix", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/role/pg_reserved", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/role/postgres", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_Role_Create(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterRoleHandlers(router, "/api", manager.Manager)

	t.Run("CreateSuccess", func(t *testing.T) {
		body := `{"name": "test_http_create_role"}`
		req := httptest.NewRequest(http.MethodPost, "/api/role", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusCreated, w.Code)

		var resp schema.Role
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal("test_http_create_role", resp.Name)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteRole(req.Context(), "test_http_create_role")
		})
	})

	t.Run("CreateEmptyName", func(t *testing.T) {
		body := `{"name": ""}`
		req := httptest.NewRequest(http.MethodPost, "/api/role", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateReservedPrefix", func(t *testing.T) {
		body := `{"name": "pg_reserved_test"}`
		req := httptest.NewRequest(http.MethodPost, "/api/role", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateInvalidJSON", func(t *testing.T) {
		body := `{invalid json}`
		req := httptest.NewRequest(http.MethodPost, "/api/role", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		// First create
		body := `{"name": "test_http_duplicate_role"}`
		req := httptest.NewRequest(http.MethodPost, "/api/role", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)
		assert.Equal(http.StatusCreated, w.Code)

		// Second create should fail
		req2 := httptest.NewRequest(http.MethodPost, "/api/role", bytes.NewBufferString(body))
		req2.Header.Set("Content-Type", "application/json")
		w2 := httptest.NewRecorder()

		router.ServeHTTP(w2, req2)
		// Duplicate role returns 500 (PostgreSQL error)
		assert.Equal(http.StatusInternalServerError, w2.Code)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteRole(req.Context(), "test_http_duplicate_role")
		})
	})
}

func Test_Role_Delete(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterRoleHandlers(router, "/api", manager.Manager)

	t.Run("DeleteExisting", func(t *testing.T) {
		// First create a role
		body := `{"name": "test_http_delete_role"}`
		createReq := httptest.NewRequest(http.MethodPost, "/api/role", bytes.NewBufferString(body))
		createReq.Header.Set("Content-Type", "application/json")
		createW := httptest.NewRecorder()
		router.ServeHTTP(createW, createReq)
		assert.Equal(http.StatusCreated, createW.Code)

		// Delete the role
		req := httptest.NewRequest(http.MethodDelete, "/api/role/test_http_delete_role", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/role/nonexistent_role_xyz", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("DeleteReservedPrefix", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/role/pg_reserved", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})
}

func Test_Role_Update(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterRoleHandlers(router, "/api", manager.Manager)

	t.Run("UpdateRename", func(t *testing.T) {
		// First create a role
		body := `{"name": "test_http_update_role_old"}`
		createReq := httptest.NewRequest(http.MethodPost, "/api/role", bytes.NewBufferString(body))
		createReq.Header.Set("Content-Type", "application/json")
		createW := httptest.NewRecorder()
		router.ServeHTTP(createW, createReq)
		assert.Equal(http.StatusCreated, createW.Code)

		// Update (rename) the role
		updateBody := `{"name": "test_http_update_role_new"}`
		req := httptest.NewRequest(http.MethodPatch, "/api/role/test_http_update_role_old", bytes.NewBufferString(updateBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		var resp schema.Role
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal("test_http_update_role_new", resp.Name)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteRole(req.Context(), "test_http_update_role_new")
		})
	})

	t.Run("UpdateNotFound", func(t *testing.T) {
		body := `{"name": "new_name"}`
		req := httptest.NewRequest(http.MethodPatch, "/api/role/nonexistent_role_xyz", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("UpdateToReservedPrefix", func(t *testing.T) {
		// First create a role
		body := `{"name": "test_http_update_reserved_role"}`
		createReq := httptest.NewRequest(http.MethodPost, "/api/role", bytes.NewBufferString(body))
		createReq.Header.Set("Content-Type", "application/json")
		createW := httptest.NewRecorder()
		router.ServeHTTP(createW, createReq)
		assert.Equal(http.StatusCreated, createW.Code)

		// Try to rename to reserved prefix
		updateBody := `{"name": "pg_reserved_name"}`
		req := httptest.NewRequest(http.MethodPatch, "/api/role/test_http_update_reserved_role", bytes.NewBufferString(updateBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteRole(req.Context(), "test_http_update_reserved_role")
		})
	})

	t.Run("UpdateReservedPrefixSource", func(t *testing.T) {
		body := `{"name": "new_name"}`
		req := httptest.NewRequest(http.MethodPatch, "/api/role/pg_reserved", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("UpdateInvalidJSON", func(t *testing.T) {
		body := `{invalid json}`
		req := httptest.NewRequest(http.MethodPatch, "/api/role/postgres", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})
}
