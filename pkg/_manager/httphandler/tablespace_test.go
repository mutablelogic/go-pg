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

func Test_Tablespace_RegisterHandlers(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	t.Run("PanicOnNilManager", func(t *testing.T) {
		router := http.NewServeMux()
		assert.Panics(func() {
			httprequest.RegisterTablespaceHandlers(router, "/api", nil)
		})
	})

	t.Run("RegisterSuccess", func(t *testing.T) {
		router := http.NewServeMux()
		assert.NotPanics(func() {
			httprequest.RegisterTablespaceHandlers(router, "/api", manager.Manager)
		})
	})
}

func Test_Tablespace_List(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterTablespaceHandlers(router, "/api", manager.Manager)

	t.Run("ListTablespaces", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/tablespace", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.TablespaceList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.GreaterOrEqual(int(resp.Count), 2) // At least pg_default and pg_global
	})

	t.Run("ListTablespacesWithPagination", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/tablespace?limit=1", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		var resp schema.TablespaceList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.LessOrEqual(len(resp.Body), 1)
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/tablespace", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_Tablespace_Get(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterTablespaceHandlers(router, "/api", manager.Manager)

	t.Run("GetPgDefault", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/tablespace/pg_default", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		// pg_default has pg_ prefix, so it's rejected by the handler
		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("GetNotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/tablespace/nonexistent_ts_xyz", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("GetReservedPrefix", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/tablespace/pg_reserved", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/tablespace/mytablespace", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_Tablespace_Create(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterTablespaceHandlers(router, "/api", manager.Manager)

	t.Run("CreateEmptyName", func(t *testing.T) {
		body := `{"name": "", "location": "/tmp/test"}`
		req := httptest.NewRequest(http.MethodPost, "/api/tablespace", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateReservedPrefix", func(t *testing.T) {
		body := `{"name": "pg_reserved_test", "location": "/tmp/test"}`
		req := httptest.NewRequest(http.MethodPost, "/api/tablespace", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateEmptyLocation", func(t *testing.T) {
		body := `{"name": "test_tablespace", "location": ""}`
		req := httptest.NewRequest(http.MethodPost, "/api/tablespace", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateRelativeLocation", func(t *testing.T) {
		body := `{"name": "test_tablespace", "location": "relative/path"}`
		req := httptest.NewRequest(http.MethodPost, "/api/tablespace", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateInvalidJSON", func(t *testing.T) {
		body := `{invalid json}`
		req := httptest.NewRequest(http.MethodPost, "/api/tablespace", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})
}

func Test_Tablespace_Delete(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterTablespaceHandlers(router, "/api", manager.Manager)

	t.Run("DeleteNotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/tablespace/nonexistent_ts_xyz", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("DeleteReservedPrefix", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/tablespace/pg_default", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})
}

func Test_Tablespace_Update(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterTablespaceHandlers(router, "/api", manager.Manager)

	t.Run("UpdateNotFound", func(t *testing.T) {
		body := `{"name": "new_name"}`
		req := httptest.NewRequest(http.MethodPatch, "/api/tablespace/nonexistent_ts_xyz", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("UpdateReservedPrefix", func(t *testing.T) {
		body := `{"name": "new_name"}`
		req := httptest.NewRequest(http.MethodPatch, "/api/tablespace/pg_default", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("UpdateInvalidJSON", func(t *testing.T) {
		body := `{invalid json}`
		req := httptest.NewRequest(http.MethodPatch, "/api/tablespace/mytablespace", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})
}
