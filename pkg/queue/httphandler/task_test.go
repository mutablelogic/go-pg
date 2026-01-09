package httphandler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	// Packages
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	httphandler "github.com/mutablelogic/go-pg/pkg/queue/httphandler"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	test "github.com/mutablelogic/go-pg/pkg/test"
	assert "github.com/stretchr/testify/assert"
)

func Test_Task_RetainWithoutAcceptHeader(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	container, pool, err := test.NewPgxContainer(ctx, "task_retain_test", testing.Verbose(), nil)
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	mgr, err := queue.New(ctx, pool, "test_task")
	assert.NoError(err)

	// Create a queue and task
	_, err = mgr.RegisterQueue(ctx, schema.QueueMeta{Queue: "test_queue"})
	assert.NoError(err)
	defer mgr.DeleteQueue(ctx, "test_queue")

	_, err = mgr.CreateTask(ctx, "test_queue", schema.TaskMeta{Payload: json.RawMessage(`{"test": "data"}`)})
	assert.NoError(err)

	router := http.NewServeMux()
	httphandler.RegisterTaskHandlers(router, "/api", mgr)

	t.Run("RetainWithoutAcceptHeader", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/task/test_queue?worker=test_worker", nil)
		// Explicitly not setting Accept header
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if !assert.Equal(http.StatusCreated, w.Code) {
			t.Logf("Response: %s", w.Body.String())
		}
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var task schema.TaskWithStatus
		err := json.Unmarshal(w.Body.Bytes(), &task)
		assert.NoError(err)
	})

	t.Run("RetainWithAcceptHeader", func(t *testing.T) {
		// Create another task
		_, err = mgr.CreateTask(ctx, "test_queue", schema.TaskMeta{Payload: json.RawMessage(`{"test": "data2"}`)})
		assert.NoError(err)

		req := httptest.NewRequest(http.MethodGet, "/api/task/test_queue?worker=test_worker", nil)
		req.Header.Set("Accept", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if !assert.Equal(http.StatusCreated, w.Code) {
			t.Logf("Response: %s", w.Body.String())
		}
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var task schema.TaskWithStatus
		err := json.Unmarshal(w.Body.Bytes(), &task)
		assert.NoError(err)
	})
}

func Test_Task_Create(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	container, pool, err := test.NewPgxContainer(ctx, "task_create_test", testing.Verbose(), nil)
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	mgr, err := queue.New(ctx, pool, "test_task_create")
	assert.NoError(err)

	// Create a queue
	_, err = mgr.RegisterQueue(ctx, schema.QueueMeta{Queue: "test_queue"})
	assert.NoError(err)
	defer mgr.DeleteQueue(ctx, "test_queue")

	router := http.NewServeMux()
	httphandler.RegisterTaskHandlers(router, "/api", mgr)

	t.Run("CreateSuccess", func(t *testing.T) {
		body := `{"queue": "test_queue", "payload": {"test": "data"}}`
		req := httptest.NewRequest(http.MethodPost, "/api/task", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if !assert.Equal(http.StatusCreated, w.Code) {
			t.Logf("Response: %s", w.Body.String())
		}
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var task schema.Task
		err := json.Unmarshal(w.Body.Bytes(), &task)
		assert.NoError(err)
		assert.Equal("test_queue", task.Queue)
	})

	t.Run("CreateQueueNotFound", func(t *testing.T) {
		body := `{"queue": "nonexistent_queue", "payload": {"test": "data"}}`
		req := httptest.NewRequest(http.MethodPost, "/api/task", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})
}

func Test_Task_Release(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	container, pool, err := test.NewPgxContainer(ctx, "task_release_test", testing.Verbose(), nil)
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	mgr, err := queue.New(ctx, pool, "test_task_release")
	assert.NoError(err)

	// Create a queue
	_, err = mgr.RegisterQueue(ctx, schema.QueueMeta{Queue: "test_queue"})
	assert.NoError(err)
	defer mgr.DeleteQueue(ctx, "test_queue")

	router := http.NewServeMux()
	httphandler.RegisterTaskHandlers(router, "/api", mgr)

	t.Run("ReleaseWithPatch", func(t *testing.T) {
		// Create and retain a task
		task, err := mgr.CreateTask(ctx, "test_queue", schema.TaskMeta{Payload: json.RawMessage(`{"test": "data"}`)})
		assert.NoError(err)
		task, err = mgr.NextTask(ctx, "test_queue", "test_worker")
		assert.NoError(err)

		// Release with result
		body := `{"result": {"status": "completed"}}`
		req := httptest.NewRequest(http.MethodPatch, "/api/task/"+strconv.FormatUint(task.Id, 10), bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if !assert.Equal(http.StatusCreated, w.Code) {
			t.Logf("Response: %s", w.Body.String())
		}
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var taskWithStatus schema.TaskWithStatus
		err = json.Unmarshal(w.Body.Bytes(), &taskWithStatus)
		assert.NoError(err)
		assert.NotEmpty(taskWithStatus.Status)
	})

	t.Run("ReleaseNotFound", func(t *testing.T) {
		body := `{"result": {"status": "completed"}}`
		req := httptest.NewRequest(http.MethodPatch, "/api/task/99999", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})
}
