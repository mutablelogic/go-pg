package httphandler_test

import (
	"bytes"
	"context"
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

func Test_ReplicationSlot_RegisterHandlers(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	t.Run("PanicOnNilManager", func(t *testing.T) {
		router := http.NewServeMux()
		assert.Panics(func() {
			httphandler.RegisterReplicationSlotHandlers(router, "/api", nil)
		})
	})

	t.Run("RegisterSuccess", func(t *testing.T) {
		router := http.NewServeMux()
		assert.NotPanics(func() {
			httphandler.RegisterReplicationSlotHandlers(router, "/api", manager.Manager)
		})
	})
}

func Test_ReplicationSlot_List(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httphandler.RegisterReplicationSlotHandlers(router, "/api", manager.Manager)

	t.Run("ListSlots", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/replicationslot", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.ReplicationSlotList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		// Count should match body length (may be 0 initially)
		assert.Equal(len(resp.Body), int(resp.Count))
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/replicationslot", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_ReplicationSlot_Get(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httphandler.RegisterReplicationSlotHandlers(router, "/api", manager.Manager)

	t.Run("GetNotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/replicationslot/nonexistent_slot_xyz", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("GetReservedPrefix", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/replicationslot/pg_reserved", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPatch, "/api/replicationslot/test_slot", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusMethodNotAllowed, w.Code)
	})
}

func Test_ReplicationSlot_Create(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httphandler.RegisterReplicationSlotHandlers(router, "/api", manager.Manager)

	t.Run("CreatePhysicalSuccess", func(t *testing.T) {
		body := `{"name": "test_http_physical", "type": "physical"}`
		req := httptest.NewRequest(http.MethodPost, "/api/replicationslot", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusCreated, w.Code)

		var resp schema.ReplicationSlot
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal("test_http_physical", resp.Name)
		assert.Equal("physical", resp.Type)
		assert.Equal("inactive", resp.Status)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteReplicationSlot(context.TODO(), "test_http_physical")
		})
	})

	t.Run("CreateLogicalSuccess", func(t *testing.T) {
		body := `{"name": "test_http_logical", "type": "logical", "plugin": "pgoutput"}`
		req := httptest.NewRequest(http.MethodPost, "/api/replicationslot", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusCreated, w.Code)

		var resp schema.ReplicationSlot
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal("test_http_logical", resp.Name)
		assert.Equal("logical", resp.Type)
		assert.Equal("pgoutput", resp.Plugin)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteReplicationSlot(context.TODO(), "test_http_logical")
		})
	})

	t.Run("CreateEmptyName", func(t *testing.T) {
		body := `{"name": "", "type": "physical"}`
		req := httptest.NewRequest(http.MethodPost, "/api/replicationslot", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateInvalidType", func(t *testing.T) {
		body := `{"name": "test_invalid", "type": "invalid"}`
		req := httptest.NewRequest(http.MethodPost, "/api/replicationslot", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateLogicalWithoutPlugin", func(t *testing.T) {
		body := `{"name": "test_no_plugin", "type": "logical"}`
		req := httptest.NewRequest(http.MethodPost, "/api/replicationslot", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateReservedPrefix", func(t *testing.T) {
		body := `{"name": "pg_reserved_slot", "type": "physical"}`
		req := httptest.NewRequest(http.MethodPost, "/api/replicationslot", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateInvalidJSON", func(t *testing.T) {
		body := `{invalid json}`
		req := httptest.NewRequest(http.MethodPost, "/api/replicationslot", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		// First create
		body := `{"name": "test_http_duplicate", "type": "physical"}`
		req := httptest.NewRequest(http.MethodPost, "/api/replicationslot", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)
		assert.Equal(http.StatusCreated, w.Code)

		// Second create should fail
		req2 := httptest.NewRequest(http.MethodPost, "/api/replicationslot", bytes.NewBufferString(body))
		req2.Header.Set("Content-Type", "application/json")
		w2 := httptest.NewRecorder()

		router.ServeHTTP(w2, req2)
		// Duplicate slot returns 500 (PostgreSQL error)
		assert.Equal(http.StatusInternalServerError, w2.Code)

		// Cleanup
		t.Cleanup(func() {
			_, _ = manager.DeleteReplicationSlot(context.TODO(), "test_http_duplicate")
		})
	})
}

func Test_ReplicationSlot_Delete(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httphandler.RegisterReplicationSlotHandlers(router, "/api", manager.Manager)

	t.Run("DeleteExisting", func(t *testing.T) {
		// First create a slot
		body := `{"name": "test_http_delete", "type": "physical"}`
		createReq := httptest.NewRequest(http.MethodPost, "/api/replicationslot", bytes.NewBufferString(body))
		createReq.Header.Set("Content-Type", "application/json")
		createW := httptest.NewRecorder()
		router.ServeHTTP(createW, createReq)
		assert.Equal(http.StatusCreated, createW.Code)

		// Delete the slot
		req := httptest.NewRequest(http.MethodDelete, "/api/replicationslot/test_http_delete", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)

		// Verify it's deleted
		getReq := httptest.NewRequest(http.MethodGet, "/api/replicationslot/test_http_delete", nil)
		getW := httptest.NewRecorder()
		router.ServeHTTP(getW, getReq)
		assert.Equal(http.StatusNotFound, getW.Code)
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/replicationslot/nonexistent_slot_xyz", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("DeleteReservedPrefix", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/replicationslot/pg_reserved", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusBadRequest, w.Code)
	})
}

func Test_ReplicationSlot_RoundTrip(t *testing.T) {
	assert := assert.New(t)

	// Create manager with test container
	manager := test.NewManager(t)
	t.Cleanup(func() {
		manager.Close()
	})

	router := http.NewServeMux()
	httphandler.RegisterReplicationSlotHandlers(router, "/api", manager.Manager)

	t.Run("CreateGetListDelete", func(t *testing.T) {
		slotName := "test_http_roundtrip"

		// Create
		createBody := `{"name": "` + slotName + `", "type": "physical"}`
		createReq := httptest.NewRequest(http.MethodPost, "/api/replicationslot", bytes.NewBufferString(createBody))
		createReq.Header.Set("Content-Type", "application/json")
		createW := httptest.NewRecorder()
		router.ServeHTTP(createW, createReq)
		assert.Equal(http.StatusCreated, createW.Code)

		// Get
		getReq := httptest.NewRequest(http.MethodGet, "/api/replicationslot/"+slotName, nil)
		getW := httptest.NewRecorder()
		router.ServeHTTP(getW, getReq)
		assert.Equal(http.StatusOK, getW.Code)

		var slot schema.ReplicationSlot
		err := json.Unmarshal(getW.Body.Bytes(), &slot)
		assert.NoError(err)
		assert.Equal(slotName, slot.Name)
		assert.Equal("physical", slot.Type)
		assert.Equal("inactive", slot.Status)

		// List should include it
		listReq := httptest.NewRequest(http.MethodGet, "/api/replicationslot", nil)
		listW := httptest.NewRecorder()
		router.ServeHTTP(listW, listReq)
		assert.Equal(http.StatusOK, listW.Code)

		var list schema.ReplicationSlotList
		err = json.Unmarshal(listW.Body.Bytes(), &list)
		assert.NoError(err)
		found := false
		for _, s := range list.Body {
			if s.Name == slotName {
				found = true
				break
			}
		}
		assert.True(found, "slot should appear in list")

		// Delete
		deleteReq := httptest.NewRequest(http.MethodDelete, "/api/replicationslot/"+slotName, nil)
		deleteW := httptest.NewRecorder()
		router.ServeHTTP(deleteW, deleteReq)
		assert.Equal(http.StatusOK, deleteW.Code)

		// Get should now return 404
		getReq2 := httptest.NewRequest(http.MethodGet, "/api/replicationslot/"+slotName, nil)
		getW2 := httptest.NewRecorder()
		router.ServeHTTP(getW2, getReq2)
		assert.Equal(http.StatusNotFound, getW2.Code)
	})
}
