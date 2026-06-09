package manager

import (
	"context"

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
