package httphandler_test

import (
	"context"
	"encoding/json"
	"io"
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

func Test_Namespace_Handler(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	container, pool, err := test.NewPgxContainer(ctx, "namespace_handler_test", testing.Verbose(), nil)
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	// Create multiple managers with different namespaces
	mgr1, err := queue.New(ctx, pool, "namespace1")
	assert.NoError(err)

	mgr2, err := queue.New(ctx, pool, "namespace2")
	assert.NoError(err)

	mgr3, err := queue.New(ctx, pool, "namespace3")
	assert.NoError(err)

	// Create a queue in each namespace to ensure they appear in the database
	_, err = mgr1.RegisterQueue(ctx, schema.QueueMeta{Queue: "queue1"})
	assert.NoError(err)

	_, err = mgr2.RegisterQueue(ctx, schema.QueueMeta{Queue: "queue2"})
	assert.NoError(err)

	_, err = mgr3.RegisterQueue(ctx, schema.QueueMeta{Queue: "queue3"})
	assert.NoError(err)

	// Create HTTP server with namespace handler
	router := http.NewServeMux()
	httphandler.RegisterNamespaceHandlers(router, "/api", mgr1)

	server := httptest.NewServer(router)
	defer server.Close()

	t.Run("ListNamespaces", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/api/namespace")
		assert.NoError(err)
		defer resp.Body.Close()

		assert.Equal(http.StatusOK, resp.StatusCode)
		assert.Contains(resp.Header.Get("Content-Type"), "application/json")

		body, err := io.ReadAll(resp.Body)
		assert.NoError(err)

		var result schema.NamespaceList
		err = json.Unmarshal(body, &result)
		assert.NoError(err)

		// Should have at least our 3 namespaces (plus potentially pgqueue system namespace)
		assert.GreaterOrEqual(len(result.Body), 3)
		assert.Contains(result.Body, "namespace1")
		assert.Contains(result.Body, "namespace2")
		assert.Contains(result.Body, "namespace3")

		// Verify count matches body length
		assert.Equal(uint64(len(result.Body)), result.Count)
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPost, server.URL+"/api/namespace", nil)
		assert.NoError(err)

		resp, err := http.DefaultClient.Do(req)
		assert.NoError(err)
		defer resp.Body.Close()

		assert.Equal(http.StatusMethodNotAllowed, resp.StatusCode)
	})
}
