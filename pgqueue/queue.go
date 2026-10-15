package pgqueue

import (
	"context"
	"errors"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterQueue creates a new queue, or updates an existing queue, and returns it.
func (self *Manager) RegisterQueue(ctx context.Context, name string, meta schema.QueueMeta, callback schema.TaskFunc) (result *schema.Queue, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.RegisterQueue",
		attribute.String("name", name),
		attribute.String("meta", types.Stringify(meta)),
		attribute.Bool("callback", callback != nil),
	)
	defer func() { endSpan(err) }()

	var queue schema.Queue

	// Register task callback if provided
	if err := self.queues.RegisterTask(name, callback); err != nil {
		return nil, err
	}

	err = self.Tx(ctx, func(conn pg.Conn) error {
		err := conn.Get(ctx, &queue, schema.QueueName(name))
		switch {
		case err == nil:
			// Queue already exists, optionally patch below.
		case errors.Is(err, pg.ErrNotFound):
			if err := conn.Insert(ctx, &queue, schema.Queue{Queue: name}); err != nil {
				return err
			}
		default:
			return err
		}

		if !hasQueueMetaPatch(meta) {
			return nil
		}

		return conn.Update(ctx, &queue, schema.QueueName(name), schema.Queue{Queue: name, QueueMeta: meta})
	})
	if err != nil {
		return nil, errors.Join(err, self.queues.RemoveTask(name))
	}

	// Return success
	return types.Ptr(queue), nil
}

// ListQueues returns all queues as a list.
func (self *Manager) ListQueues(ctx context.Context, req schema.QueueListRequest) (result *schema.QueueList, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.ListQueues",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	resp := schema.QueueList{QueueListRequest: req}
	if err := self.List(ctx, &resp, req); err != nil {
		return nil, err
	} else {
		resp.OffsetLimit.Clamp(resp.Count)
	}
	return types.Ptr(resp), nil
}

// GetQueue returns a queue by name.
func (self *Manager) GetQueue(ctx context.Context, name string) (result *schema.Queue, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.GetQueue",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	var queue schema.Queue
	if err := self.Get(ctx, &queue, schema.QueueName(name)); err != nil {
		return nil, err
	}
	return types.Ptr(queue), nil
}

// DeleteQueue deletes an existing queue, and returns it.
func (self *Manager) DeleteQueue(ctx context.Context, name string) (result *schema.Queue, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.DeleteQueue",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	// Delete the queue, and its task callback. A queue registered by another
	// process sharing the database has no callback in this one.
	var queue schema.Queue
	if err := self.Delete(ctx, &queue, schema.QueueName(name)); err != nil {
		return nil, err
	} else if err := self.queues.RemoveTask(name); err != nil && !errors.Is(err, httpresponse.ErrNotFound) {
		return nil, err
	}
	return types.Ptr(queue), nil
}

// UpdateQueue updates an existing queue, and returns it.
func (self *Manager) UpdateQueue(ctx context.Context, name string, meta schema.QueueMeta) (result *schema.Queue, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.UpdateQueue",
		attribute.String("name", name),
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	var queue schema.Queue
	if err := self.Update(ctx, &queue, schema.QueueName(name), schema.Queue{Queue: name, QueueMeta: meta}); err != nil {
		return nil, err
	}
	return types.Ptr(queue), nil
}

// CleanQueue removes up to schema.QueueCleanLimit of the queue's finished
// tasks, which finished longer ago than the retention period, and returns the
// tasks removed.
func (self *Manager) CleanQueue(ctx context.Context, name string) (result []schema.Task, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.CleanQueue",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	var resp schema.QueueCleanResponse
	if err := self.List(ctx, &resp, schema.QueueCleanRequest{Queue: name, Retention: self.retention}); err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// ListQueueStatuses returns the status breakdown for all queues.
func (self *Manager) ListQueueStatuses(ctx context.Context) (result []schema.QueueStatus, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.ListQueueStatuses")
	defer func() { endSpan(err) }()

	var resp schema.QueueStatusResponse
	if err := self.List(ctx, &resp, schema.QueueStatusRequest{}); err != nil {
		return nil, err
	}
	return resp.Body, nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func hasQueueMetaPatch(meta schema.QueueMeta) bool {
	return meta.TTL != nil || meta.Retries != nil || meta.RetryDelay != nil || meta.Concurrency != nil || meta.Dedupe != nil
}
