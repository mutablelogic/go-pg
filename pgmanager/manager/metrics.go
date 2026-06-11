package manager

import (
	"context"
	"strings"

	// Packages

	otel "github.com/mutablelogic/go-client/pkg/otel"
	"github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
	metric "go.opentelemetry.io/otel/metric"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) RegisterDatabaseMetrics(name string) (err error) {
	// Register a gauge for database size
	if guage, err := manager.metrics.Int64ObservableGauge(
		name, metric.WithDescription("Size of database in bytes"),
	); err != nil {
		return pg.ErrInternalServerError.With("RegisterDatabaseMetrics: %w", err)
	} else if _, err := manager.metrics.RegisterCallback(func(parent context.Context, observer metric.Observer) error {
		// Otel span
		ctx, endSpan := otel.StartSpan(manager.tracer, parent, "ObserveDatabaseMetrics",
			attribute.String("name", name),
		)
		defer func() { endSpan(err) }()

		// TODO: Paginate through databases
		databases, err := manager.ListDatabases(ctx, schema.DatabaseListRequest{})
		if err != nil {
			return pg.ErrInternalServerError.With("RegisterDatabaseMetrics: %w", err)
		}
		for _, database := range databases.Body {
			observer.ObserveInt64(guage, int64(database.Size), metric.WithAttributes(
				attribute.String("cluster", manager.cluster),
				attribute.String("database", database.Name),
				attribute.Int64("oid", int64(database.Oid)),
			))
		}
		return nil
	}, guage); err != nil {
		return pg.ErrInternalServerError.With("RegisterDatabaseMetrics: %w", err)
	}

	// Return success
	return nil
}

func (manager *Manager) RegisterSchemaMetrics(name string) error {
	if guage, err := manager.metrics.Int64ObservableGauge(
		name, metric.WithDescription("Size of schema in bytes"),
	); err != nil {
		return pg.ErrInternalServerError.With("RegisterSchemaMetrics: %w", err)
	} else if _, err := manager.metrics.RegisterCallback(func(parent context.Context, observer metric.Observer) error {
		// Otel span
		ctx, endSpan := otel.StartSpan(manager.tracer, parent, "ObserveSchemaMetrics",
			attribute.String("name", name),
		)
		defer func() { endSpan(err) }()

		// TODO: Paginate through schemas
		schemas, err := manager.ListSchemas(ctx, schema.SchemaListRequest{})
		if err != nil {
			return pg.ErrInternalServerError.With("RegisterSchemaMetrics: %w", err)
		}
		for _, schema := range schemas.Body {
			observer.ObserveInt64(guage, int64(schema.Size), metric.WithAttributes(
				attribute.String("cluster", manager.cluster),
				attribute.String("database", schema.Database),
				attribute.String("schema", schema.Name),
				attribute.Int64("oid", int64(schema.Oid)),
			))
		}
		return nil
	}, guage); err != nil {
		return pg.ErrInternalServerError.With("RegisterSchemaMetrics: %w", err)
	}

	// Return success
	return nil
}

func (manager *Manager) RegisterConnectionMetrics(name string) error {
	if guage, err := manager.metrics.Int64ObservableGauge(
		name, metric.WithDescription("Number of active connections"),
	); err != nil {
		return pg.ErrInternalServerError.With("RegisterConnectionMetrics: %w", err)
	} else if _, err := manager.metrics.RegisterCallback(func(parent context.Context, observer metric.Observer) error {
		// Otel span
		ctx, endSpan := otel.StartSpan(manager.tracer, parent, "ObserveConnectionMetrics",
			attribute.String("name", name),
		)
		defer func() { endSpan(err) }()

		// TODO: Paginate through connections
		connections, err := manager.ListConnections(ctx, schema.ConnectionListRequest{})
		if err != nil {
			return pg.ErrInternalServerError.With("RegisterConnectionMetrics: %w", err)
		}

		type key struct {
			database string
			role     string
			state    string
		}
		counts := make(map[key]int64)
		for _, connection := range connections.Body {
			k := key{
				database: strings.TrimSpace(connection.Database),
				role:     strings.TrimSpace(connection.Role),
				state:    strings.TrimSpace(connection.State),
			}
			if k.database == "" {
				k.database = "unknown"
			}
			if k.role == "" {
				k.role = "unknown"
			}
			if k.state == "" {
				k.state = "unknown"
			}
			counts[k]++
		}

		for k, count := range counts {
			observer.ObserveInt64(guage, count, metric.WithAttributes(
				attribute.String("cluster", manager.cluster),
				attribute.String("database", k.database),
				attribute.String("role", k.role),
				attribute.String("state", k.state),
			))
		}
		return nil
	}, guage); err != nil {
		return pg.ErrInternalServerError.With("RegisterConnectionMetrics: %w", err)
	}

	// Return success
	return nil
}

func (manager *Manager) RegisterTablespaceMetrics(name string) error {
	if guage, err := manager.metrics.Int64ObservableGauge(
		name, metric.WithDescription("Size of tablespace in bytes"),
	); err != nil {
		return pg.ErrInternalServerError.With("RegisterTablespaceMetrics: %w", err)
	} else if _, err := manager.metrics.RegisterCallback(func(parent context.Context, observer metric.Observer) error {
		// Otel span
		ctx, endSpan := otel.StartSpan(manager.tracer, parent, "ObserveTablespaceMetrics",
			attribute.String("name", name),
		)
		defer func() { endSpan(err) }()

		// TODO: Paginate through tablespaces
		tablespaces, err := manager.ListTablespaces(ctx, schema.TablespaceListRequest{})
		if err != nil {
			return pg.ErrInternalServerError.With("RegisterTablespaceMetrics: %w", err)
		}
		for _, tablespace := range tablespaces.Body {
			observer.ObserveInt64(guage, int64(tablespace.Size), metric.WithAttributes(
				attribute.String("cluster", manager.cluster),
				attribute.String("tablespace", tablespace.Name),
				attribute.Int64("oid", int64(tablespace.Oid)),
				attribute.String("location", tablespace.Location),
			))
		}
		return nil
	}, guage); err != nil {
		return pg.ErrInternalServerError.With("RegisterTablespaceMetrics: %w", err)
	}

	// Return success
	return nil
}

func (manager *Manager) RegisterReplicationSlotMetrics(name string) error {
	lag_bytes, err := manager.metrics.Int64ObservableGauge(
		name+"_bytes", metric.WithDescription("Lag of replication slot in bytes"),
	)
	if err != nil {
		return pg.ErrInternalServerError.With("RegisterReplicationSlotMetrics: %w", err)
	}
	lag_ms, err := manager.metrics.Float64ObservableGauge(
		name+"_ms", metric.WithDescription("Lag of replication slot in milliseconds"),
	)
	if err != nil {
		return pg.ErrInternalServerError.With("RegisterReplicationSlotMetrics: %w", err)
	}

	if _, err := manager.metrics.RegisterCallback(func(parent context.Context, observer metric.Observer) error {
		// Otel span
		ctx, endSpan := otel.StartSpan(manager.tracer, parent, "ObserveReplicationSlotMetrics",
			attribute.String("name", name),
		)
		defer func() { endSpan(err) }()

		// TODO: Paginate through replication slots
		replicationslots, err := manager.ListReplicationSlots(ctx, schema.ReplicationSlotListRequest{})
		if err != nil {
			return pg.ErrInternalServerError.With("RegisterReplicationSlotMetrics: %w", err)
		}
		for _, slot := range replicationslots.Body {
			if slot.LagBytes != nil {
				observer.ObserveInt64(lag_bytes, types.Value(slot.LagBytes), metric.WithAttributes(
					attribute.String("cluster", manager.cluster),
					attribute.String("slot", slot.Name),
					attribute.String("database", slot.Database),
					attribute.String("type", slot.Type),
					attribute.String("status", slot.Status),
				))
			}
			if slot.LagMs != nil {
				observer.ObserveFloat64(lag_ms, types.Value(slot.LagMs), metric.WithAttributes(
					attribute.String("cluster", manager.cluster),
					attribute.String("slot", slot.Name),
					attribute.String("database", slot.Database),
					attribute.String("type", slot.Type),
					attribute.String("status", slot.Status),
				))
			}
		}
		return nil
	}, lag_bytes, lag_ms); err != nil {
		return pg.ErrInternalServerError.With("RegisterReplicationSlotMetrics: %w", err)
	}

	// Return success
	return nil
}
