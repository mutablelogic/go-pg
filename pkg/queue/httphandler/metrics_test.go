package httphandler_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	// Packages
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	httphandler "github.com/mutablelogic/go-pg/pkg/queue/httphandler"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	test "github.com/mutablelogic/go-pg/pkg/test"
	assert "github.com/stretchr/testify/assert"
)

func Test_Metrics_Handler(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	container, pool, err := test.NewPgxContainer(ctx, "metrics_handler_test", testing.Verbose(), nil)
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	// Create manager
	mgr, err := queue.New(ctx, pool, queue.WithNamespace("test_metrics"))
	assert.NoError(err)

	// Create some queues
	ttl := 1 * time.Hour
	_, err = mgr.RegisterQueue(ctx, schema.QueueMeta{
		Queue: "test-queue",
		TTL:   &ttl,
	})
	assert.NoError(err)

	// Create some tasks
	_, err = mgr.CreateTask(ctx, "test-queue", schema.TaskMeta{
		Payload: map[string]string{"test": "data"},
	})
	assert.NoError(err)

	// Create HTTP handler
	router := http.NewServeMux()
	httphandler.RegisterMetricsHandler(router, "/api", mgr, nil)

	// Create test server
	server := httptest.NewServer(router)
	defer server.Close()

	t.Run("GetMetrics", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/api/metrics")
		assert.NoError(err)
		defer resp.Body.Close()

		assert.Equal(http.StatusOK, resp.StatusCode)
		assert.Contains(resp.Header.Get("Content-Type"), "text/plain; version=0.0.4; charset=utf-8")

		body, err := io.ReadAll(resp.Body)
		assert.NoError(err)

		bodyStr := string(body)

		// Print sample output for demonstration
		t.Logf("Sample Prometheus metrics output:\n%s", bodyStr)

		// Verify the metric exists
		assert.Contains(bodyStr, "queue_tasks")

		// Verify metric has correct labels
		assert.Contains(bodyStr, `namespace="test_metrics"`)
		assert.Contains(bodyStr, `queue="test-queue"`)

		// Verify we have at least some status types
		assert.Contains(bodyStr, `status="new"`)

		// Verify HELP and TYPE lines
		assert.Contains(bodyStr, "# HELP queue_tasks Number of tasks in each queue by status")
		assert.Contains(bodyStr, "# TYPE queue_tasks gauge")
	})

	t.Run("MethodNotAllowed", func(t *testing.T) {
		resp, err := http.Post(server.URL+"/api/metrics", "application/json", strings.NewReader("{}"))
		assert.NoError(err)
		defer resp.Body.Close()

		assert.Equal(http.StatusMethodNotAllowed, resp.StatusCode)
	})
}

func Test_Metrics_WithMultipleQueues(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	container, pool, err := test.NewPgxContainer(ctx, "metrics_multi_queues_test", testing.Verbose(), nil)
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	// Create manager
	mgr, err := queue.New(ctx, pool, queue.WithNamespace("test_multi_metrics"))
	assert.NoError(err)

	// Create multiple queues
	for i := 1; i <= 3; i++ {
		_, err = mgr.RegisterQueue(ctx, schema.QueueMeta{
			Queue: "queue-" + string(rune('0'+i)),
		})
		assert.NoError(err)
	}

	// Create tasks in different queues
	_, err = mgr.CreateTask(ctx, "queue-1", schema.TaskMeta{
		Payload: map[string]string{"queue": "1"},
	})
	assert.NoError(err)

	_, err = mgr.CreateTask(ctx, "queue-2", schema.TaskMeta{
		Payload: map[string]string{"queue": "2"},
	})
	assert.NoError(err)

	// Create HTTP handler
	router := http.NewServeMux()
	httphandler.RegisterMetricsHandler(router, "/", mgr, nil)

	// Create test server
	server := httptest.NewServer(router)
	defer server.Close()

	t.Run("MultipleQueuesMetrics", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/metrics")
		assert.NoError(err)
		defer resp.Body.Close()

		assert.Equal(http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		assert.NoError(err)

		bodyStr := string(body)

		// Verify all queues appear
		assert.Contains(bodyStr, `queue="queue-1"`)
		assert.Contains(bodyStr, `queue="queue-2"`)
		assert.Contains(bodyStr, `queue="queue-3"`)

		// Verify all status types appear (at least new should be there)
		assert.Contains(bodyStr, `status="new"`)
		assert.Contains(bodyStr, `status="retained"`)
		assert.Contains(bodyStr, `status="released"`)
	})
}
