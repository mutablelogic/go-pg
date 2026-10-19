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
		queues, err := self.ListQueueStatuses(ctx)
		if err != nil {
			return err
		}
		for _, queue := range queues {
			for status, count := range map[string]uint64{
				"new":     queue.New,
				"running": queue.Running,
				"retry":   queue.Retry,
				"expired": queue.Expired,
				"failed":  queue.Failed,
				"done":    queue.Done,
			} {
				observer.ObserveInt64(queueTasks, int64(count),
					metric.WithAttributes(
						attribute.String("queue", queue.Name),
						attribute.String("status", status),
						attribute.String("worker", self.opt.worker),
					),
				)
			}
		}
		return nil
	}, queueTasks); err != nil {
		return err
	}

	return nil
}
