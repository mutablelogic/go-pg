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
		// Insert the queue, unless it exists, such as when another process
		// sharing the database registers it at the same time, and then get it
		if err := conn.Insert(ctx, &queue, schema.Queue{Name: name}); errors.Is(err, pg.ErrNotFound) {
			if err := conn.Get(ctx, &queue, schema.QueueName(name)); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}

		if !hasQueueMetaPatch(meta) {
			return nil
		}

		return conn.Update(ctx, &queue, schema.QueueName(name), schema.Queue{Name: name, QueueMeta: meta})
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

	// Copy the limit, so the caller's request isn't modified
	if req.Limit != nil {
		req.Limit = types.Ptr(*req.Limit)
	}

	// Selecting sets the limit used on the request, which is returned,
	// clamped to the number of rows available
	var resp schema.QueueList
	if err := self.List(ctx, &resp, &req); err != nil {
		return nil, err
	} else {
		resp.QueueListRequest = req
		resp.OffsetLimit.Clamp(resp.Count)
	}
	return types.Ptr(resp), nil
}

// GetQueue returns a queue by name.
func (self *Manager) GetQueue(ctx context.Context, name string) (result *schema.QueueWithStatus, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.GetQueue",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	var queue schema.QueueWithStatus
	if err := self.Get(ctx, &queue, schema.QueueStatusRequest{Queue: name}); err != nil {
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

	// Delete the queue. When it can't be deleted, it's returned, and its
	// callback is kept, as the queue and its tasks may still exist.
	var queue schema.Queue
	deleteErr := self.Delete(ctx, &queue, schema.QueueName(name))
	if deleteErr != nil && !errors.Is(deleteErr, pg.ErrNotFound) {
		return nil, deleteErr
	}

	// Remove its task callback, also when another process sharing the
	// database deleted the queue first, so it can be registered again. A
	// queue registered by another process has no callback in this one.
	if err := self.removeCallback(name); err != nil && !errors.Is(err, httpresponse.ErrNotFound) {
		return nil, errors.Join(deleteErr, err)
	} else if deleteErr != nil {
		return nil, deleteErr
	}
	return types.Ptr(queue), nil
}

// UnregisterQueue removes the task callback of the queue in this process, so
// this process stops running its tasks, but keeps the queue and its tasks,
// for other processes, or for when the queue is registered again. Tasks which
// are running are released when they finish. Returns ErrNotFound if the
// queue has no callback in this process.
func (self *Manager) UnregisterQueue(ctx context.Context, name string) (err error) {
	_, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.UnregisterQueue",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	return self.removeCallback(name)
}

// UpdateQueue updates an existing queue, and returns it.
func (self *Manager) UpdateQueue(ctx context.Context, name string, meta schema.QueueMeta) (result *schema.Queue, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.UpdateQueue",
		attribute.String("name", name),
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	var queue schema.Queue
	if err := self.Update(ctx, &queue, schema.QueueName(name), schema.Queue{Name: name, QueueMeta: meta}); err != nil {
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

// ListQueueStatuses returns every queue, with the number of its tasks with
// each status
func (self *Manager) ListQueueStatuses(ctx context.Context) (result []schema.QueueWithStatus, err error) {
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

// removeCallback removes the task callback of the queue, once no task is
// being retained and run, so the run loop doesn't retain a task from the
// queue without its callback
func (self *Manager) removeCallback(name string) error {
	self.dispatch.Lock()
	defer self.dispatch.Unlock()
	return self.queues.RemoveTask(name)
}
