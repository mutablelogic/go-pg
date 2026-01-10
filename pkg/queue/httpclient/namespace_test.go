package httpclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	// Packages
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	httpclient "github.com/mutablelogic/go-pg/pkg/queue/httpclient"
	httphandler "github.com/mutablelogic/go-pg/pkg/queue/httphandler"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	test "github.com/mutablelogic/go-pg/pkg/test"
	assert "github.com/stretchr/testify/assert"
)

func Test_HTTPClient_ListNamespaces(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	container, pool, err := test.NewPgxContainer(ctx, "httpclient_namespace_test", testing.Verbose(), nil)
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	// Create multiple managers with different namespaces
	mgr1, err := queue.New(ctx, pool, queue.WithNamespace("ns1"))
	assert.NoError(err)

	mgr2, err := queue.New(ctx, pool, queue.WithNamespace("ns2"))
	assert.NoError(err)

	// Create a queue in each namespace
	_, err = mgr1.RegisterQueue(ctx, schema.QueueMeta{Queue: "queue1"})
	assert.NoError(err)

	_, err = mgr2.RegisterQueue(ctx, schema.QueueMeta{Queue: "queue2"})
	assert.NoError(err)

	// Create HTTP server
	router := http.NewServeMux()
	httphandler.RegisterBackendHandlers(router, "/api", mgr1, nil)

	server := httptest.NewServer(router)
	defer server.Close()

	// Create HTTP client with /api prefix
	client, err := httpclient.New(server.URL + "/api")
	assert.NoError(err)

	// List namespaces
	namespaces, err := client.ListNamespaces(ctx)
	if !assert.NoError(err) {
		return
	}
	if !assert.NotNil(namespaces) {
		return
	}

	// Should have at least our 2 namespaces (plus potentially pgqueue system namespace)
	assert.GreaterOrEqual(len(namespaces.Body), 2)
	assert.Contains(namespaces.Body, "ns1")
	assert.Contains(namespaces.Body, "ns2")

	// Verify count matches body length
	assert.Equal(uint64(len(namespaces.Body)), namespaces.Count)

	t.Logf("Found namespaces: %v", namespaces.Body)
}
