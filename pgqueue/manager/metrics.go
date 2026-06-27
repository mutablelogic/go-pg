package manager

import (
	"context"

	// Packages
	attribute "go.opentelemetry.io/otel/attribute"
	metric "go.opentelemetry.io/otel/metric"
)

func (manager *Manager) registerMetrics() error {
	if manager.opt.metrics == nil {
		return nil
	}

	queueTasks, err := manager.opt.metrics.Int64ObservableGauge(
		"pgqueue_tasks",
		metric.WithDescription("Current number of queue tasks by queue and status"),
	)
	if err != nil {
		return err
	}
	if _, err := manager.opt.metrics.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		statuses, err := manager.ListQueueStatuses(ctx)
		if err != nil {
			return err
		}
		for _, status := range statuses {
			observer.ObserveInt64(queueTasks, int64(status.Count),
				metric.WithAttributes(
					attribute.String("queue", status.Queue),
					attribute.String("status", status.Status),
					attribute.String("worker", manager.opt.worker),
				),
			)
		}
		return nil
	}, queueTasks); err != nil {
		return err
	}

	return nil
}
