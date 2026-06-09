package httphandler

import (
	"context"
	"net/http"
	"sync"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pkg/manager"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
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
	manager             *manager.Manager
	connections         *prometheus.Desc
	databaseSize        *prometheus.Desc
	tablespaceSize      *prometheus.Desc
	tableSize           *prometheus.Desc
	indexSize           *prometheus.Desc
	deadTupleRatio      *prometheus.Desc
	replicationSlots    *prometheus.Desc
	replicationLagBytes *prometheus.Desc
	replicationLagMs    *prometheus.Desc
}

// RegisterMetricsHandler registers a HTTP handler for prometheus metrics
// on the provided router with the given path prefix. The manager must be non-nil.
func RegisterMetricsHandler(router *http.ServeMux, prefix string, manager *manager.Manager) {
	if manager == nil {
		panic("manager is nil")
	}

	// Create a prometheus registry
	registry := prometheus.NewRegistry()
	registry.MustRegister(&metrics{
		manager: manager,
		connections: prometheus.NewDesc(
			"pg_connections",
			"Number of connections to the database server",
			[]string{"database", "state"}, nil,
		),
		databaseSize: prometheus.NewDesc(
			"pg_database_size_bytes",
			"Size of database in bytes",
			[]string{"database"}, nil,
		),
		tablespaceSize: prometheus.NewDesc(
			"pg_tablespace_size_bytes",
			"Size of tablespace in bytes",
			[]string{"tablespace"}, nil,
		),
		tableSize: prometheus.NewDesc(
			"pg_table_size_bytes",
			"Size of table in bytes",
			[]string{"database", "schema", "table"}, nil,
		),
		indexSize: prometheus.NewDesc(
			"pg_index_size_bytes",
			"Size of index in bytes",
			[]string{"database", "schema", "index"}, nil,
		),
		deadTupleRatio: prometheus.NewDesc(
			"pg_table_dead_tuple_ratio",
			"Ratio of dead tuples to total tuples (0.0-1.0)",
			[]string{"database", "schema", "table"}, nil,
		),
		replicationSlots: prometheus.NewDesc(
			"pg_replication_slots",
			"Number of replication slots by status",
			[]string{"status"}, nil,
		),
		replicationLagBytes: prometheus.NewDesc(
			"pg_replication_lag_bytes",
			"Replication lag in bytes",
			[]string{"slot", "type"}, nil,
		),
		replicationLagMs: prometheus.NewDesc(
			"pg_replication_lag_ms",
			"Replication lag in milliseconds",
			[]string{"slot", "type"}, nil,
		),
	})
	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})

	// Create a handler for metrics
	router.HandleFunc(joinPath(prefix, "metrics"), func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handler.ServeHTTP(w, r)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - COLLECTOR

// Describe sends metric descriptors to the channel
func (m *metrics) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.connections
	ch <- m.databaseSize
	ch <- m.tablespaceSize
	ch <- m.tableSize
	ch <- m.indexSize
	ch <- m.deadTupleRatio
	ch <- m.replicationSlots
	ch <- m.replicationLagBytes
	ch <- m.replicationLagMs
}

// Collect fetches metrics from the database and sends them to the channel
func (m *metrics) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), metricsTimeout)
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := m.collectConnections(ctx, ch); err != nil {
			ch <- prometheus.NewInvalidMetric(m.connections, err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := m.collectDatabaseSize(ctx, ch); err != nil {
			ch <- prometheus.NewInvalidMetric(m.databaseSize, err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := m.collectTablespaceSize(ctx, ch); err != nil {
			ch <- prometheus.NewInvalidMetric(m.tablespaceSize, err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := m.collectObjectSize(ctx, ch); err != nil {
			ch <- prometheus.NewInvalidMetric(m.tableSize, err)
			ch <- prometheus.NewInvalidMetric(m.indexSize, err)
			ch <- prometheus.NewInvalidMetric(m.deadTupleRatio, err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := m.collectReplicationSlots(ctx, ch); err != nil {
			ch <- prometheus.NewInvalidMetric(m.replicationSlots, err)
			ch <- prometheus.NewInvalidMetric(m.replicationLagBytes, err)
			ch <- prometheus.NewInvalidMetric(m.replicationLagMs, err)
		}
	}()

	wg.Wait()
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (m *metrics) collectConnections(ctx context.Context, ch chan<- prometheus.Metric) error {
	// Count connections by database and state
	counts := make(map[string]map[string]float64)

	// Paginate through all connections
	var offset uint64
	for {
		req := schema.ConnectionListRequest{
			OffsetLimit: pg.OffsetLimit{
				Offset: offset,
			},
		}

		// Get connections
		list, err := m.manager.ListConnections(ctx, req)
		if err != nil {
			return err
		}

		// Increment counts
		for _, conn := range list.Body {
			if counts[conn.Database] == nil {
				counts[conn.Database] = make(map[string]float64)
			}
			counts[conn.Database][conn.State]++
		}

		// Check if we've fetched all connections
		offset += uint64(len(list.Body))
		if offset >= list.Count || len(list.Body) == 0 {
			break
		}
	}

	// Send metrics for each database/state combination
	for database, states := range counts {
		for state, count := range states {
			ch <- prometheus.MustNewConstMetric(m.connections, prometheus.GaugeValue, count, database, state)
		}
	}

	return nil
}

func (m *metrics) collectDatabaseSize(ctx context.Context, ch chan<- prometheus.Metric) error {
	// Paginate through all databases
	var offset uint64
	for {
		req := schema.DatabaseListRequest{
			OffsetLimit: pg.OffsetLimit{
				Offset: offset,
			},
		}

		list, err := m.manager.ListDatabases(ctx, req)
		if err != nil {
			return err
		}

		for _, db := range list.Body {
			ch <- prometheus.MustNewConstMetric(m.databaseSize, prometheus.GaugeValue, float64(db.Size), db.Name)
		}

		// Check if we've fetched all databases
		offset += uint64(len(list.Body))
		if offset >= list.Count || len(list.Body) == 0 {
			break
		}
	}

	return nil
}

func (m *metrics) collectTablespaceSize(ctx context.Context, ch chan<- prometheus.Metric) error {
	// Paginate through all tablespaces
	var offset uint64
	for {
		req := schema.TablespaceListRequest{
			OffsetLimit: pg.OffsetLimit{
				Offset: offset,
			},
		}

		list, err := m.manager.ListTablespaces(ctx, req)
		if err != nil {
			return err
		}

		for _, ts := range list.Body {
			ch <- prometheus.MustNewConstMetric(m.tablespaceSize, prometheus.GaugeValue, float64(ts.Size), ts.Name)
		}

		// Check if we've fetched all tablespaces
		offset += uint64(len(list.Body))
		if offset >= list.Count || len(list.Body) == 0 {
			break
		}
	}

	return nil
}

func (m *metrics) collectObjectSize(ctx context.Context, ch chan<- prometheus.Metric) error {
	// Paginate through all objects
	var offset uint64
	for {
		req := schema.ObjectListRequest{
			OffsetLimit: pg.OffsetLimit{
				Offset: offset,
			},
		}

		list, err := m.manager.ListObjects(ctx, req)
		if err != nil {
			return err
		}

		for _, obj := range list.Body {
			switch obj.Type {
			case "TABLE", "PARTITIONED TABLE":
				ch <- prometheus.MustNewConstMetric(m.tableSize, prometheus.GaugeValue, float64(obj.Size), obj.Database, obj.Schema, obj.Name)
				// Calculate dead tuple ratio if we have tuple data
				if obj.Table != nil && obj.Table.LiveTuples != nil && obj.Table.DeadTuples != nil {
					live := *obj.Table.LiveTuples
					dead := *obj.Table.DeadTuples
					total := live + dead
					if total > 0 {
						ratio := float64(dead) / float64(total)
						ch <- prometheus.MustNewConstMetric(m.deadTupleRatio, prometheus.GaugeValue, ratio, obj.Database, obj.Schema, obj.Name)
					}
				}
			case "INDEX", "PARTITIONED INDEX":
				ch <- prometheus.MustNewConstMetric(m.indexSize, prometheus.GaugeValue, float64(obj.Size), obj.Database, obj.Schema, obj.Name)
			}
		}

		// Check if we've fetched all objects
		offset += uint64(len(list.Body))
		if offset >= list.Count || len(list.Body) == 0 {
			break
		}
	}

	return nil
}

func (m *metrics) collectReplicationSlots(ctx context.Context, ch chan<- prometheus.Metric) error {
	// Count slots by status
	statusCounts := make(map[string]float64)

	// Paginate through all replication slots
	var offset uint64
	for {
		req := schema.ReplicationSlotListRequest{
			OffsetLimit: pg.OffsetLimit{
				Offset: offset,
			},
		}

		list, err := m.manager.ListReplicationSlots(ctx, req)
		if err != nil {
			return err
		}

		for _, slot := range list.Body {
			// Count by status
			statusCounts[slot.Status]++

			// Report lag metrics for active slots
			if slot.LagBytes != nil {
				ch <- prometheus.MustNewConstMetric(m.replicationLagBytes, prometheus.GaugeValue, float64(*slot.LagBytes), slot.Name, slot.Type)
			}
			if slot.LagMs != nil {
				ch <- prometheus.MustNewConstMetric(m.replicationLagMs, prometheus.GaugeValue, *slot.LagMs, slot.Name, slot.Type)
			}
		}

		// Check if we've fetched all slots
		offset += uint64(len(list.Body))
		if offset >= list.Count || len(list.Body) == 0 {
			break
		}
	}

	// Send slot count metrics by status
	for status, count := range statusCounts {
		ch <- prometheus.MustNewConstMetric(m.replicationSlots, prometheus.GaugeValue, count, status)
	}

	return nil
}
