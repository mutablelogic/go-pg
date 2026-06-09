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

func Test_Schema_RegisterHandlers(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	t.Run("PanicOnNilManager", func(t *testing.T) {
		router := http.NewServeMux()
		assert.Panics(func() {
			httprequest.RegisterSchemaHandlers(router, "/api", nil)
		})
	})

	t.Run("RegisterSuccess", func(t *testing.T) {
		router := http.NewServeMux()
		assert.NotPanics(func() {
			httprequest.RegisterSchemaHandlers(router, "/api", manager.Manager)
		})
	})
}

func Test_Schema_List(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterSchemaHandlers(router, "/api", manager.Manager)

	t.Run("ListAllSchemas", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/schema", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.SchemaList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.GreaterOrEqual(int(resp.Count), 1) // At least the public schema
	})

	t.Run("ListSchemasByDatabase", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/schema/postgres", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.SchemaList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.GreaterOrEqual(int(resp.Count), 1) // At least the public schema

		// All schemas should be from the postgres database
		for _, s := range resp.Body {
			assert.Equal("postgres", s.Database)
		}
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/schema", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_Schema_Get(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterSchemaHandlers(router, "/api", manager.Manager)

	t.Run("GetExisting", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/schema/postgres/public", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.Schema
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal("public", resp.Name)
		assert.Equal("postgres", resp.Database)
	})

	t.Run("GetNotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/schema/postgres/nonexistent_schema_xyz", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("GetReservedPrefix", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/schema/postgres/pg_reserved", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/schema/postgres/public", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_Schema_Create(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterSchemaHandlers(router, "/api", manager.Manager)

	t.Run("CreateSuccess", func(t *testing.T) {
		body := `{"name": "test_http_schema", "owner": "postgres"}`
		req := httptest.NewRequest(http.MethodPost, "/api/schema/postgres", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusCreated, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.Schema
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal("test_http_schema", resp.Name)
		assert.Equal("postgres", resp.Database)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteSchema(req.Context(), "postgres", "test_http_schema", true)
		})
	})

	t.Run("CreateWithDefaultOwner", func(t *testing.T) {
		body := `{"name": "test_http_schema_noowner"}`
		req := httptest.NewRequest(http.MethodPost, "/api/schema/postgres", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		// Without owner, should succeed and default to postgres user
		assert.Equal(http.StatusCreated, w.Code)

		var resp schema.Schema
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal("test_http_schema_noowner", resp.Name)
		// Owner defaults to the current connection user
		assert.NotEmpty(resp.Owner)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteSchema(req.Context(), "postgres", "test_http_schema_noowner", true)
		})
	})

	t.Run("CreateWithOwner", func(t *testing.T) {
		body := `{"name": "test_http_schema_owner", "owner": "postgres"}`
		req := httptest.NewRequest(http.MethodPost, "/api/schema/postgres", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusCreated, w.Code)

		var resp schema.Schema
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal("test_http_schema_owner", resp.Name)
		assert.Equal("postgres", resp.Owner)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteSchema(req.Context(), "postgres", "test_http_schema_owner", true)
		})
	})

	t.Run("CreateEmptyName", func(t *testing.T) {
		body := `{"name": ""}`
		req := httptest.NewRequest(http.MethodPost, "/api/schema/postgres", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateReservedPrefix", func(t *testing.T) {
		body := `{"name": "pg_reserved_schema"}`
		req := httptest.NewRequest(http.MethodPost, "/api/schema/postgres", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateInvalidJSON", func(t *testing.T) {
		body := `{invalid json}`
		req := httptest.NewRequest(http.MethodPost, "/api/schema/postgres", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		// First create
		body := `{"name": "test_http_duplicate_schema", "owner": "postgres"}`
		req := httptest.NewRequest(http.MethodPost, "/api/schema/postgres", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)
		assert.Equal(http.StatusCreated, w.Code)

		// Second create should fail
		req2 := httptest.NewRequest(http.MethodPost, "/api/schema/postgres", bytes.NewBufferString(body))
		req2.Header.Set("Content-Type", "application/json")
		w2 := httptest.NewRecorder()

		router.ServeHTTP(w2, req2)
		// Duplicate schema returns 500 (PostgreSQL error)
		assert.Equal(http.StatusInternalServerError, w2.Code)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteSchema(req.Context(), "postgres", "test_http_duplicate_schema", true)
		})
	})
}

func Test_Schema_Delete(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterSchemaHandlers(router, "/api", manager.Manager)

	t.Run("DeleteExisting", func(t *testing.T) {
		// First create a schema
		body := `{"name": "test_http_delete_schema", "owner": "postgres"}`
		createReq := httptest.NewRequest(http.MethodPost, "/api/schema/postgres", bytes.NewBufferString(body))
		createReq.Header.Set("Content-Type", "application/json")
		createW := httptest.NewRecorder()
		router.ServeHTTP(createW, createReq)
		assert.Equal(http.StatusCreated, createW.Code)

		// Delete the schema
		req := httptest.NewRequest(http.MethodDelete, "/api/schema/postgres/test_http_delete_schema", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
	})

	t.Run("DeleteWithForce", func(t *testing.T) {
		// First create a schema
		body := `{"name": "test_http_delete_force_schema", "owner": "postgres"}`
		createReq := httptest.NewRequest(http.MethodPost, "/api/schema/postgres", bytes.NewBufferString(body))
		createReq.Header.Set("Content-Type", "application/json")
		createW := httptest.NewRecorder()
		router.ServeHTTP(createW, createReq)
		assert.Equal(http.StatusCreated, createW.Code)

		// Delete with force
		req := httptest.NewRequest(http.MethodDelete, "/api/schema/postgres/test_http_delete_force_schema?force=true", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/schema/postgres/nonexistent_schema_xyz", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("DeleteReservedPrefix", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/schema/postgres/pg_reserved", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})
}

func Test_Schema_Update(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterSchemaHandlers(router, "/api", manager.Manager)

	t.Run("UpdateRename", func(t *testing.T) {
		// First create a schema
		body := `{"name": "test_http_update_schema_old", "owner": "postgres"}`
		createReq := httptest.NewRequest(http.MethodPost, "/api/schema/postgres", bytes.NewBufferString(body))
		createReq.Header.Set("Content-Type", "application/json")
		createW := httptest.NewRecorder()
		router.ServeHTTP(createW, createReq)
		assert.Equal(http.StatusCreated, createW.Code)

		// Update (rename) the schema
		updateBody := `{"name": "test_http_update_schema_new"}`
		req := httptest.NewRequest(http.MethodPatch, "/api/schema/postgres/test_http_update_schema_old", bytes.NewBufferString(updateBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		var resp schema.Schema
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal("test_http_update_schema_new", resp.Name)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteSchema(req.Context(), "postgres", "test_http_update_schema_new", true)
		})
	})

	t.Run("UpdateOwner", func(t *testing.T) {
		// First create a schema
		body := `{"name": "test_http_update_owner_schema", "owner": "postgres"}`
		createReq := httptest.NewRequest(http.MethodPost, "/api/schema/postgres", bytes.NewBufferString(body))
		createReq.Header.Set("Content-Type", "application/json")
		createW := httptest.NewRecorder()
		router.ServeHTTP(createW, createReq)
		assert.Equal(http.StatusCreated, createW.Code)

		// Update owner
		updateBody := `{"owner": "postgres"}`
		req := httptest.NewRequest(http.MethodPatch, "/api/schema/postgres/test_http_update_owner_schema", bytes.NewBufferString(updateBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		var resp schema.Schema
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal("postgres", resp.Owner)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteSchema(req.Context(), "postgres", "test_http_update_owner_schema", true)
		})
	})

	t.Run("UpdateNotFound", func(t *testing.T) {
		body := `{"name": "new_name"}`
		req := httptest.NewRequest(http.MethodPatch, "/api/schema/postgres/nonexistent_schema_xyz", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("UpdateToReservedPrefix", func(t *testing.T) {
		// First create a schema
		body := `{"name": "test_http_update_reserved_schema", "owner": "postgres"}`
		createReq := httptest.NewRequest(http.MethodPost, "/api/schema/postgres", bytes.NewBufferString(body))
		createReq.Header.Set("Content-Type", "application/json")
		createW := httptest.NewRecorder()
		router.ServeHTTP(createW, createReq)
		assert.Equal(http.StatusCreated, createW.Code)

		// Try to rename to reserved prefix - PostgreSQL rejects this
		updateBody := `{"name": "pg_reserved_name"}`
		req := httptest.NewRequest(http.MethodPatch, "/api/schema/postgres/test_http_update_reserved_schema", bytes.NewBufferString(updateBody))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		// PostgreSQL returns an error for reserved prefix names
		assert.Equal(http.StatusInternalServerError, w.Code)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteSchema(req.Context(), "postgres", "test_http_update_reserved_schema", true)
		})
	})

	t.Run("UpdateReservedPrefixSource", func(t *testing.T) {
		body := `{"name": "new_name"}`
		req := httptest.NewRequest(http.MethodPatch, "/api/schema/postgres/pg_reserved", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("UpdateInvalidJSON", func(t *testing.T) {
		body := `{invalid json}`
		req := httptest.NewRequest(http.MethodPatch, "/api/schema/postgres/public", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})
}
