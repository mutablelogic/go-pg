package httphandler

import (
	"context"
	"net/http"
	"time"

	// Packages
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	prometheus "github.com/prometheus/client_golang/prometheus"
	promhttp "github.com/prometheus/client_golang/prometheus/promhttp"
)

///////////////////////////////////////////////////////////////////////////////
// CONSTANTS

const (
	metricsTimeout = 30 * time.Second
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

type metrics struct {
	manager    *queue.Manager
	queueTasks *prometheus.Desc
}

// RegisterMetricsHandler registers a HTTP handler for prometheus metrics
// on the provided router with the given path prefix. The manager must be non-nil.
func RegisterMetricsHandler(router *http.ServeMux, prefix string, manager *queue.Manager, middleware HTTPMiddlewareFuncs) {
	if manager == nil {
		panic("manager is nil")
	}

	// Create a prometheus registry
	registry := prometheus.NewRegistry()
	registry.MustRegister(&metrics{
		manager: manager,
		queueTasks: prometheus.NewDesc(
			"queue_tasks",
			"Number of tasks in each queue by status",
			[]string{"namespace", "queue", "status"}, nil,
		),
	})
	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})

	// Create a handler for metrics
	router.HandleFunc(joinPath(prefix, "metrics"), middleware.Wrap(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handler.ServeHTTP(w, r)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	}))
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - COLLECTOR

// Describe sends metric descriptors to the channel
func (m *metrics) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.queueTasks
}

// Collect fetches metrics from the database and sends them to the channel
func (m *metrics) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), metricsTimeout)
	defer cancel()

	if err := m.collectQueueStatuses(ctx, ch); err != nil {
		ch <- prometheus.NewInvalidMetric(m.queueTasks, err)
	}
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (m *metrics) collectQueueStatuses(ctx context.Context, ch chan<- prometheus.Metric) error {
	// Get all queue statuses
	statuses, err := m.manager.ListQueueStatuses(ctx)
	if err != nil {
		return err
	}

	// Get the namespace from the manager
	namespace := m.manager.Namespace()

	// Send metrics for each queue/status combination
	for _, status := range statuses {
		ch <- prometheus.MustNewConstMetric(
			m.queueTasks,
			prometheus.GaugeValue,
			float64(status.Count),
			namespace,
			status.Queue,
			status.Status,
		)
	}

	return nil
}
