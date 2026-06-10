package manager

import (
	"context"
	"strings"

	// Packages
	"github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	attribute "go.opentelemetry.io/otel/attribute"
	metric "go.opentelemetry.io/otel/metric"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) RegisterDatabaseMetrics(name string) error {
	if guage, err := manager.metrics.Int64ObservableGauge(
		name, metric.WithDescription("Size of database in bytes"),
	); err != nil {
		return pg.ErrInternalServerError.With("RegisterDatabaseMetrics: %w", err)
	} else if _, err := manager.metrics.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
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
	} else if _, err := manager.metrics.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
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
	} else if _, err := manager.metrics.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
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
