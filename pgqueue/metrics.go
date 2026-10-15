package pgqueue

import (
	"context"

	// Packages
	attribute "go.opentelemetry.io/otel/attribute"
	metric "go.opentelemetry.io/otel/metric"
)

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// registerMetrics registers the pgqueue_tasks gauge, which is the number of
// tasks in each queue with each status, when the manager has a meter
func (self *Manager) registerMetrics() error {
	if self.opt.metrics == nil {
		return nil
	}

	queueTasks, err := self.opt.metrics.Int64ObservableGauge(
		"pgqueue_tasks",
		metric.WithDescription("Current number of queue tasks by queue and status"),
	)
	if err != nil {
		return err
	}
	if _, err := self.opt.metrics.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		statuses, err := self.ListQueueStatuses(ctx)
		if err != nil {
			return err
		}
		for _, status := range statuses {
			observer.ObserveInt64(queueTasks, int64(status.Count),
				metric.WithAttributes(
					attribute.String("queue", status.Queue),
					attribute.String("status", status.Status),
					attribute.String("worker", self.opt.worker),
				),
			)
		}
		return nil
	}, queueTasks); err != nil {
		return err
	}

	return nil
}
