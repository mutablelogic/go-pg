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

func Test_Client_ListQueues(t *testing.T) {
	assert := assert.New(t)

	t.Run("EmptyList", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("GET", r.Method)
			assert.Equal("/queue", r.URL.Path)

			response := schema.QueueList{
				Count: 0,
				Body:  []schema.Queue{},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		queues, err := client.ListQueues(context.Background())
		assert.NoError(err)
		assert.NotNil(queues)
		assert.Equal(uint64(0), queues.Count)
		assert.Empty(queues.Body)
	})

	t.Run("WithQueues", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("GET", r.Method)
			assert.Equal("/queue", r.URL.Path)

			ttl := 5 * time.Minute
			retries := uint64(3)
			retryDelay := 10 * time.Second

			response := schema.QueueList{
				Count: 2,
				Body: []schema.Queue{
					{
						QueueMeta: schema.QueueMeta{
							Queue:      "queue1",
							TTL:        &ttl,
							Retries:    &retries,
							RetryDelay: &retryDelay,
						},
						Namespace: "default",
					},
					{
						QueueMeta: schema.QueueMeta{
							Queue:   "queue2",
							Retries: &retries,
						},
						Namespace: "default",
					},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		queues, err := client.ListQueues(context.Background())
		assert.NoError(err)
		assert.NotNil(queues)
		assert.Equal(uint64(2), queues.Count)
		assert.Len(queues.Body, 2)
		assert.Equal("queue1", queues.Body[0].Queue)
		assert.Equal("queue2", queues.Body[1].Queue)
	})

	t.Run("WithPagination", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("GET", r.Method)
			assert.Equal("/queue", r.URL.Path)
			assert.Equal("10", r.URL.Query().Get("offset"))
			assert.Equal("5", r.URL.Query().Get("limit"))

			response := schema.QueueList{
				Count: 0,
				Body:  []schema.Queue{},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		limit := uint64(5)
		queues, err := client.ListQueues(context.Background(), httpclient.WithOffsetLimit(10, &limit))
		assert.NoError(err)
		assert.NotNil(queues)
	})
}

func Test_Client_GetQueue(t *testing.T) {
	assert := assert.New(t)

	t.Run("ExistingQueue", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("GET", r.Method)
			assert.Equal("/queue/test-queue", r.URL.Path)

			ttl := 5 * time.Minute
			retries := uint64(3)
			retryDelay := 10 * time.Second

			response := schema.Queue{
				QueueMeta: schema.QueueMeta{
					Queue:      "test-queue",
					TTL:        &ttl,
					Retries:    &retries,
					RetryDelay: &retryDelay,
				},
				Namespace: "default",
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		queue, err := client.GetQueue(context.Background(), "test-queue")
		assert.NoError(err)
		assert.NotNil(queue)
		assert.Equal("test-queue", queue.Queue)
		assert.Equal("default", queue.Namespace)
		assert.NotNil(queue.TTL)
		assert.Equal(5*time.Minute, *queue.TTL)
	})

	t.Run("NotFoundQueue", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("GET", r.Method)
			assert.Equal("/queue/non-existent", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		queue, err := client.GetQueue(context.Background(), "non-existent")
		assert.Error(err)
		assert.Nil(queue)
	})
}

func Test_Client_CreateQueue(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidQueue", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("POST", r.Method)
			assert.Equal("/queue", r.URL.Path)

			var request schema.QueueMeta
			err := json.NewDecoder(r.Body).Decode(&request)
			assert.NoError(err)
			assert.Equal("new-queue", request.Queue)

			response := schema.Queue{
				QueueMeta: request,
				Namespace: "default",
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		ttl := 10 * time.Minute
		retries := uint64(5)
		retryDelay := 30 * time.Second
		meta := schema.QueueMeta{
			Queue:      "new-queue",
			TTL:        &ttl,
			Retries:    &retries,
			RetryDelay: &retryDelay,
		}

		queue, err := client.CreateQueue(context.Background(), meta)
		assert.NoError(err)
		assert.NotNil(queue)
		assert.Equal("new-queue", queue.Queue)
		assert.Equal("default", queue.Namespace)
	})

	t.Run("MinimalQueue", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("POST", r.Method)

			var request schema.QueueMeta
			err := json.NewDecoder(r.Body).Decode(&request)
			assert.NoError(err)
			assert.Equal("minimal-queue", request.Queue)

			response := schema.Queue{
				QueueMeta: request,
				Namespace: "default",
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		meta := schema.QueueMeta{
			Queue: "minimal-queue",
		}

		queue, err := client.CreateQueue(context.Background(), meta)
		assert.NoError(err)
		assert.NotNil(queue)
		assert.Equal("minimal-queue", queue.Queue)
	})
}

func Test_Client_UpdateQueue(t *testing.T) {
	assert := assert.New(t)

	t.Run("UpdateTTL", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("PATCH", r.Method)
			assert.Equal("/queue/test-queue", r.URL.Path)

			var request schema.QueueMeta
			err := json.NewDecoder(r.Body).Decode(&request)
			assert.NoError(err)

			response := schema.Queue{
				QueueMeta: request,
				Namespace: "default",
			}
			response.Queue = "test-queue"
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		newTTL := 15 * time.Minute
		meta := schema.QueueMeta{
			TTL: &newTTL,
		}

		queue, err := client.UpdateQueue(context.Background(), "test-queue", meta)
		assert.NoError(err)
		assert.NotNil(queue)
		assert.Equal("test-queue", queue.Queue)
		assert.NotNil(queue.TTL)
		assert.Equal(15*time.Minute, *queue.TTL)
	})
}

func Test_Client_DeleteQueue(t *testing.T) {
	assert := assert.New(t)

	t.Run("DeleteExisting", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("DELETE", r.Method)
			assert.Equal("/queue/test-queue", r.URL.Path)

			response := schema.Queue{
				QueueMeta: schema.QueueMeta{
					Queue: "test-queue",
				},
				Namespace: "default",
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		queue, err := client.DeleteQueue(context.Background(), "test-queue")
		assert.NoError(err)
		assert.NotNil(queue)
		assert.Equal("test-queue", queue.Queue)
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("DELETE", r.Method)
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		queue, err := client.DeleteQueue(context.Background(), "non-existent")
		assert.Error(err)
		assert.Nil(queue)
	})
}
