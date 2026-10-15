package pgqueue_test

import (
	"encoding/json"
	"testing"

	// Packages
	pgqueue "github.com/mutablelogic/go-pg/pgqueue"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	test "github.com/mutablelogic/go-pg/pgqueue/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	metricdata "go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMetrics(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)

	// A manager with a meter, and a queue with a new task and a running task
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(ctx) })
	manager := test.IsolatedManager(t, pgqueue.WithMeter(provider.Meter("test")), pgqueue.WithWorker("worker"))
	_, err := manager.RegisterQueue(ctx, "metrics", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)
	for range 2 {
		_, err := manager.CreateTask(ctx, "metrics", schema.TaskMeta{Payload: json.RawMessage(`{}`)})
		require.NoError(t, err)
	}
	_, err = manager.NextTask(ctx, "worker")
	require.NoError(t, err)

	// The gauge has the number of tasks by queue and status
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))
	counts := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "pgqueue_tasks" {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok, "pgqueue_tasks is %T", m.Data)
			for _, point := range gauge.DataPoints {
				queue, _ := point.Attributes.Value("queue")
				status, _ := point.Attributes.Value("status")
				worker, _ := point.Attributes.Value("worker")
				assert.Equal(t, "worker", worker.AsString())
				counts[queue.AsString()+"/"+status.AsString()] = point.Value
			}
		}
	}
	assert.Equal(t, int64(1), counts["metrics/new"])
	assert.Equal(t, int64(1), counts["metrics/running"])
	assert.Equal(t, int64(0), counts["metrics/done"])
}
