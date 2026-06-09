package httphandler_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	// Packages
	httprequest "github.com/mutablelogic/go-pg/pkg/manager/httphandler"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	test "github.com/mutablelogic/go-pg/pkg/test"
	assert "github.com/stretchr/testify/assert"
)

///////////////////////////////////////////////////////////////////////////////
// TESTS

func Test_Connection_RegisterHandlers(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	t.Run("PanicOnNilManager", func(t *testing.T) {
		router := http.NewServeMux()
		assert.Panics(func() {
			httprequest.RegisterConnectionHandlers(router, "/api", nil)
		})
	})

	t.Run("RegisterSuccess", func(t *testing.T) {
		router := http.NewServeMux()
		assert.NotPanics(func() {
			httprequest.RegisterConnectionHandlers(router, "/api", manager.Manager)
		})
	})
}

func Test_Connection_List(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterConnectionHandlers(router, "/api", manager.Manager)

	t.Run("ListConnections", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/connection", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.ConnectionList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.GreaterOrEqual(int(resp.Count), 1) // At least our own connection
	})

	t.Run("ListConnectionsWithPagination", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/connection?limit=1", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		var resp schema.ConnectionList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.LessOrEqual(len(resp.Body), 1)
	})

	t.Run("ListConnectionsByDatabase", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/connection?database=postgres", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		var resp schema.ConnectionList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)

		// All connections should be from the postgres database
		for _, conn := range resp.Body {
			assert.Equal("postgres", conn.Database)
		}
	})

	t.Run("ListConnectionsByState", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/connection?state=active", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		var resp schema.ConnectionList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)

		// All connections should have active state
		for _, conn := range resp.Body {
			assert.Equal("active", conn.State)
		}
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/connection", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_Connection_Get(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterConnectionHandlers(router, "/api", manager.Manager)

	t.Run("GetExisting", func(t *testing.T) {
		// First list connections to get a valid PID
		listReq := httptest.NewRequest(http.MethodGet, "/api/connection", nil)
		listW := httptest.NewRecorder()
		router.ServeHTTP(listW, listReq)

		var listResp schema.ConnectionList
		err := json.Unmarshal(listW.Body.Bytes(), &listResp)
		if !assert.NoError(err) || listResp.Count == 0 {
			t.Skip("No connections available to test")
		}

		pid := listResp.Body[0].Pid
		req := httptest.NewRequest(http.MethodGet, "/api/connection/"+strconv.Itoa(int(pid)), nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.Connection
		err = json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal(pid, resp.Pid)
	})

	t.Run("GetNotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/connection/999999999", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("GetInvalidPid", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/connection/invalid", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("GetZeroPid", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/connection/0", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/connection/123", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_Connection_Delete(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httprequest.RegisterConnectionHandlers(router, "/api", manager.Manager)

	t.Run("DeleteInvalidPid", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/connection/invalid", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("DeleteZeroPid", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/connection/0", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	// Note: We don't test DeleteConnection with a valid PID here because
	// terminating connections during tests could be disruptive.
	// The manager-level tests verify the functionality.
}
