package httphandler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	// Packages
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	httphandler "github.com/mutablelogic/go-pg/pkg/queue/httphandler"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	test "github.com/mutablelogic/go-pg/pkg/test"
	assert "github.com/stretchr/testify/assert"
)

func Test_Queue_List(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	container, pool, err := test.NewPgxContainer(ctx, "queue_list_test", testing.Verbose(), nil)
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	mgr, err := queue.New(ctx, pool, "test_list")
	assert.NoError(err)

	// Create test queues
	for i := 1; i <= 5; i++ {
		queueName := fmt.Sprintf("test_queue_%d", i)
		_, err := mgr.RegisterQueue(ctx, schema.QueueMeta{Queue: queueName})
		assert.NoError(err)
		defer mgr.DeleteQueue(ctx, queueName)
	}

	router := http.NewServeMux()
	httphandler.RegisterQueueHandlers(router, "/api", mgr)

	t.Run("ListQueues", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/queue", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.QueueList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.GreaterOrEqual(int(resp.Count), 5)
	})

	t.Run("ListQueuesWithLimit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/queue?limit=2", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.QueueList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal(2, len(resp.Body))
	})

	t.Run("ListQueuesWithOffset", func(t *testing.T) {
		// Get all queues first
		req := httptest.NewRequest(http.MethodGet, "/api/queue", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		var allResp schema.QueueList
		err := json.Unmarshal(w.Body.Bytes(), &allResp)
		assert.NoError(err)

		// Get with offset=2
		req = httptest.NewRequest(http.MethodGet, "/api/queue?offset=2", nil)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		var resp schema.QueueList
		err = json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal(len(allResp.Body)-2, len(resp.Body))
	})

	t.Run("ListQueuesWithLimitAndOffset", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/queue?limit=2&offset=1", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.QueueList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal(2, len(resp.Body))
	})
}

func Test_Queue_Create(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	container, pool, err := test.NewPgxContainer(ctx, "queue_create_test", testing.Verbose(), nil)
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	mgr, err := queue.New(ctx, pool, "test_create")
	assert.NoError(err)

	router := http.NewServeMux()
	httphandler.RegisterQueueHandlers(router, "/api", mgr)

	t.Run("CreateSuccess", func(t *testing.T) {
		body := `{"queue": "test_http_create", "retries": 3, "ttl": 300000000000}`
		req := httptest.NewRequest(http.MethodPost, "/api/queue", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if !assert.Equal(http.StatusCreated, w.Code) {
			t.Logf("Response: %s", w.Body.String())
		}

		defer mgr.DeleteQueue(ctx, "test_http_create")
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		body := `{"queue": "test_duplicate", "retries": 3, "ttl": 300000000000}`
		req := httptest.NewRequest(http.MethodPost, "/api/queue", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)
		if !assert.Equal(http.StatusCreated, w.Code) {
			t.Logf("Response: %s", w.Body.String())
		}

		req = httptest.NewRequest(http.MethodPost, "/api/queue", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w = httptest.NewRecorder()

		router.ServeHTTP(w, req)
		if !assert.Equal(http.StatusConflict, w.Code) {
			t.Logf("Response: %s", w.Body.String())
		}

		defer mgr.DeleteQueue(ctx, "test_duplicate")
	})
}

func Test_Queue_Get(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	container, pool, err := test.NewPgxContainer(ctx, "queue_get_test", testing.Verbose(), nil)
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	mgr, err := queue.New(ctx, pool, "test_get")
	assert.NoError(err)

	router := http.NewServeMux()
	httphandler.RegisterQueueHandlers(router, "/api", mgr)

	t.Run("GetExisting", func(t *testing.T) {
		// Create a queue first
		body := `{"queue": "test_get_queue", "retries": 3, "ttl": 300000000000}`
		req := httptest.NewRequest(http.MethodPost, "/api/queue", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(http.StatusCreated, w.Code)
		defer mgr.DeleteQueue(ctx, "test_get_queue")

		// Get the queue
		req = httptest.NewRequest(http.MethodGet, "/api/queue/test_get_queue", nil)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var q schema.Queue
		err := json.Unmarshal(w.Body.Bytes(), &q)
		assert.NoError(err)
		assert.Equal("test_get_queue", q.Queue)
	})

	t.Run("GetNotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/queue/nonexistent", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})
}

func Test_Queue_Update(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	container, pool, err := test.NewPgxContainer(ctx, "queue_update_test", testing.Verbose(), nil)
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	mgr, err := queue.New(ctx, pool, "test_update")
	assert.NoError(err)

	router := http.NewServeMux()
	httphandler.RegisterQueueHandlers(router, "/api", mgr)

	t.Run("UpdateSuccess", func(t *testing.T) {
		// Create a queue first
		body := `{"queue": "test_update_queue", "retries": 3, "ttl": 300000000000}`
		req := httptest.NewRequest(http.MethodPost, "/api/queue", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(http.StatusCreated, w.Code)
		defer mgr.DeleteQueue(ctx, "test_update_queue")

		// Update the queue
		updateBody := `{"queue": "test_update_queue", "retries": 5, "ttl": 600000000000}`
		req = httptest.NewRequest(http.MethodPatch, "/api/queue/test_update_queue", bytes.NewBufferString(updateBody))
		req.Header.Set("Content-Type", "application/json")
		w = httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var q schema.Queue
		err := json.Unmarshal(w.Body.Bytes(), &q)
		assert.NoError(err)
		assert.Equal("test_update_queue", q.Queue)
	})

	t.Run("UpdateNotFound", func(t *testing.T) {
		body := `{"queue": "nonexistent", "retries": 5, "ttl": 600000000000}`
		req := httptest.NewRequest(http.MethodPatch, "/api/queue/nonexistent", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})
}

func Test_Queue_Delete(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	container, pool, err := test.NewPgxContainer(ctx, "queue_delete_test", testing.Verbose(), nil)
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	mgr, err := queue.New(ctx, pool, "test_delete")
	assert.NoError(err)

	router := http.NewServeMux()
	httphandler.RegisterQueueHandlers(router, "/api", mgr)

	t.Run("DeleteSuccess", func(t *testing.T) {
		// Create a queue first
		body := `{"queue": "test_delete_queue", "retries": 3, "ttl": 300000000000}`
		req := httptest.NewRequest(http.MethodPost, "/api/queue", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(http.StatusCreated, w.Code)

		// Delete the queue
		req = httptest.NewRequest(http.MethodDelete, "/api/queue/test_delete_queue", nil)
		w = httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var q schema.Queue
		err := json.Unmarshal(w.Body.Bytes(), &q)
		assert.NoError(err)
		assert.Equal("test_delete_queue", q.Queue)

		// Verify queue is deleted
		req = httptest.NewRequest(http.MethodGet, "/api/queue/test_delete_queue", nil)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/queue/nonexistent", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})
}
