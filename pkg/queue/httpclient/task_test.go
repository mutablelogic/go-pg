package httpclient_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	// Packages
	httpclient "github.com/mutablelogic/go-pg/pkg/queue/httpclient"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_Client_RetainTask(t *testing.T) {
	assert := assert.New(t)

	t.Run("GetNextTask", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("GET", r.Method)
			assert.Equal("/task/test-queue", r.URL.Path)
			assert.Equal("worker-001", r.URL.Query().Get("worker"))

			now := time.Now()
			retries := uint64(3)
			response := schema.TaskWithStatus{
				Task: schema.Task{
					Id:        12345,
					Queue:     "test-queue",
					Namespace: "default",
					TaskMeta: schema.TaskMeta{
						Payload: map[string]interface{}{"key": "value"},
					},
					CreatedAt: &now,
					StartedAt: &now,
					Retries:   &retries,
				},
				Status: "running",
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		task, err := client.RetainTask(context.Background(), "test-queue", "worker-001")
		assert.NoError(err)
		assert.NotNil(task)
		assert.Equal(uint64(12345), task.Id)
		assert.Equal("test-queue", task.Queue)
		assert.Equal("running", task.Status)
		assert.NotNil(task.Payload)
	})

	t.Run("NoTaskAvailable", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("GET", r.Method)
			assert.Equal("/task/empty-queue", r.URL.Path)
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		task, err := client.RetainTask(context.Background(), "empty-queue", "worker-001")
		assert.Error(err)
		assert.Nil(task)
	})

	t.Run("EmptyQueueName", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("GET", r.Method)
			// Empty queue name results in /task/ path
			assert.Equal("worker-001", r.URL.Query().Get("worker"))
			w.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		task, err := client.RetainTask(context.Background(), "", "worker-001")
		assert.Error(err)
		assert.Nil(task)
	})
}

func Test_Client_CreateTask(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidTask", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("POST", r.Method)
			assert.Equal("/task", r.URL.Path)

			var request struct {
				Queue string `json:"queue"`
				schema.TaskMeta
			}
			err := json.NewDecoder(r.Body).Decode(&request)
			assert.NoError(err)
			assert.Equal("test-queue", request.Queue)
			assert.NotNil(request.Payload)

			now := time.Now()
			response := schema.Task{
				Id:        12345,
				Queue:     request.Queue,
				Namespace: "default",
				TaskMeta:  request.TaskMeta,
				CreatedAt: &now,
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		meta := schema.TaskMeta{
			Payload: map[string]interface{}{
				"operation": "process",
				"data":      "important data",
			},
		}

		task, err := client.CreateTask(context.Background(), "test-queue", meta)
		assert.NoError(err)
		assert.NotNil(task)
		assert.Equal(uint64(12345), task.Id)
		assert.Equal("test-queue", task.Queue)
		assert.NotNil(task.Payload)
	})

	t.Run("TaskWithDelay", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("POST", r.Method)

			var request struct {
				Queue string `json:"queue"`
				schema.TaskMeta
			}
			err := json.NewDecoder(r.Body).Decode(&request)
			assert.NoError(err)
			assert.NotNil(request.DelayedAt)

			now := time.Now()
			response := schema.Task{
				Id:        12346,
				Queue:     request.Queue,
				Namespace: "default",
				TaskMeta:  request.TaskMeta,
				CreatedAt: &now,
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		delayedAt := time.Now().Add(5 * time.Minute)
		meta := schema.TaskMeta{
			Payload:   map[string]interface{}{"key": "value"},
			DelayedAt: &delayedAt,
		}

		task, err := client.CreateTask(context.Background(), "delayed-queue", meta)
		assert.NoError(err)
		assert.NotNil(task)
		assert.NotNil(task.DelayedAt)
	})

	t.Run("EmptyPayload", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var request struct {
				Queue string `json:"queue"`
				schema.TaskMeta
			}
			err := json.NewDecoder(r.Body).Decode(&request)
			assert.NoError(err)

			now := time.Now()
			response := schema.Task{
				Id:        12347,
				Queue:     request.Queue,
				Namespace: "default",
				CreatedAt: &now,
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		meta := schema.TaskMeta{}
		task, err := client.CreateTask(context.Background(), "test-queue", meta)
		assert.NoError(err)
		assert.NotNil(task)
	})
}

func Test_Client_ReleaseTask(t *testing.T) {
	assert := assert.New(t)

	t.Run("SuccessWithNilError", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("PATCH", r.Method)
			assert.Equal("/task/12345", r.URL.Path)

			var request struct {
				Result interface{} `json:"result,omitempty"`
				Fail   bool        `json:"fail,omitempty"`
			}
			err := json.NewDecoder(r.Body).Decode(&request)
			assert.NoError(err)
			assert.False(request.Fail)
			assert.Nil(request.Result)

			response := schema.TaskWithStatus{
				Task: schema.Task{
					Id: 12345,
				},
				Status: "completed",
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		task, err := client.ReleaseTask(context.Background(), 12345, nil)
		assert.NoError(err)
		assert.NotNil(task)
		assert.Equal(uint64(12345), task.Id)
		assert.Equal("completed", task.Status)
	})

	t.Run("FailureWithError", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("PATCH", r.Method)
			assert.Equal("/task/12346", r.URL.Path)

			var request struct {
				Result interface{} `json:"result,omitempty"`
				Fail   bool        `json:"fail,omitempty"`
			}
			err := json.NewDecoder(r.Body).Decode(&request)
			assert.NoError(err)
			assert.True(request.Fail)
			assert.NotNil(request.Result)

			response := schema.TaskWithStatus{
				Task: schema.Task{
					Id:     12346,
					Result: request.Result,
				},
				Status: "pending", // will retry
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		errPayload := map[string]interface{}{
			"error": "something went wrong",
		}

		task, err := client.ReleaseTask(context.Background(), 12346, errPayload)
		assert.NoError(err)
		assert.NotNil(task)
		assert.Equal(uint64(12346), task.Id)
	})

	t.Run("NotFound", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("PATCH", r.Method)
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		task, err := client.ReleaseTask(context.Background(), 99999, nil)
		assert.Error(err)
		assert.Nil(task)
	})
}
