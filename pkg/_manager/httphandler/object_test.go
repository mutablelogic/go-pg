package httphandler_test

import (
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

func Test_Object_RegisterHandlers(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	t.Run("PanicOnNilManager", func(t *testing.T) {
		router := http.NewServeMux()
		assert.Panics(func() {
			httprequest.RegisterObjectHandlers(router, "/api", nil)
		})
	})

	t.Run("RegisterSuccess", func(t *testing.T) {
		router := http.NewServeMux()
		assert.NotPanics(func() {
			httprequest.RegisterObjectHandlers(router, "/api", manager.Manager)
		})
	})
}

func Test_Object_List(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterObjectHandlers(router, "/api", manager.Manager)

	t.Run("ListAllObjects", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/object", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.ObjectList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		// Count should match or exceed body length
		assert.LessOrEqual(len(resp.Body), int(resp.Count))
	})

	t.Run("ListObjectsByDatabase", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/object/postgres", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.ObjectList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)

		// All objects should be from the postgres database
		for _, obj := range resp.Body {
			assert.Equal("postgres", obj.Database)
		}
	})

	t.Run("ListObjectsByDatabaseAndSchema", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/object/postgres/public", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.ObjectList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)

		// All objects should be from postgres.public
		for _, obj := range resp.Body {
			assert.Equal("postgres", obj.Database)
			assert.Equal("public", obj.Schema)
		}
	})

	t.Run("ListObjectsWithPagination", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/object?limit=1", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		var resp schema.ObjectList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.LessOrEqual(len(resp.Body), 1)
	})

	t.Run("ListObjectsWithTypeFilter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/object?type=TABLE", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		var resp schema.ObjectList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)

		// All returned objects should be tables
		for _, obj := range resp.Body {
			assert.Equal("TABLE", obj.Type)
		}
	})

	t.Run("ListFromNonExistentDatabase", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/object/nonexistent_db_xyz", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		var resp schema.ObjectList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		// Should return empty list
		assert.Equal(uint64(0), resp.Count)
		assert.Empty(resp.Body)
	})

	t.Run("MethodNotAllowedOnList", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/object", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})

	t.Run("MethodNotAllowedOnDatabaseList", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/object/postgres", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})

	t.Run("MethodNotAllowedOnSchemaList", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/object/postgres/public", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_Object_Get(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterObjectHandlers(router, "/api", manager.Manager)

	t.Run("GetNotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/object/postgres/public/nonexistent_object_xyz", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("GetFromNonExistentSchema", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/object/postgres/nonexistent_schema/test", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("GetExistingObject", func(t *testing.T) {
		// First, list objects to find one that exists
		listReq := httptest.NewRequest(http.MethodGet, "/api/object/postgres/public?limit=1", nil)
		listW := httptest.NewRecorder()
		router.ServeHTTP(listW, listReq)

		if listW.Code != http.StatusOK {
			t.Skip("Could not list objects")
		}

		var listResp schema.ObjectList
		if err := json.Unmarshal(listW.Body.Bytes(), &listResp); err != nil {
			t.Skip("Could not parse object list")
		}

		if len(listResp.Body) == 0 {
			t.Skip("No objects found in postgres.public")
		}

		// Get the first object
		obj := listResp.Body[0]
		getReq := httptest.NewRequest(http.MethodGet, "/api/object/"+obj.Database+"/"+obj.Schema+"/"+obj.Name, nil)
		getW := httptest.NewRecorder()
		router.ServeHTTP(getW, getReq)

		assert.Equal(http.StatusOK, getW.Code)
		assert.Contains(getW.Header().Get("Content-Type"), "application/json")

		var resp schema.Object
		err := json.Unmarshal(getW.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal(obj.Name, resp.Name)
		assert.Equal(obj.Schema, resp.Schema)
		assert.Equal(obj.Database, resp.Database)
		assert.Equal(obj.Type, resp.Type)
		assert.Equal(obj.Owner, resp.Owner)
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/object/postgres/public/test", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}
