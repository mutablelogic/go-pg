package queue

import (
	"context"
	"errors"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - QUEUE

// RegisterQueue creates a new queue, or updates an existing queue, and returns it.
func (manager *Manager) RegisterQueue(ctx context.Context, meta schema.QueueMeta) (*schema.Queue, error) {
	var queue schema.Queue
	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		// Get a queue
		if err := conn.Get(ctx, &queue, schema.QueueName(meta.Queue)); err != nil && !errors.Is(err, pg.ErrNotFound) {
			return err
		} else if errors.Is(err, pg.ErrNotFound) {
			// If the queue does not exist, then create it
			if err := conn.Insert(ctx, &queue, meta); err != nil {
				return err
			}
		}

		// Update the queue
		return conn.Update(ctx, &queue, schema.QueueName(meta.Queue), meta)
	}); err != nil {
		return nil, err
	}

	return &queue, nil
}

// ListQueues returns all queues in a namespace as a list
func (manager *Manager) ListQueues(ctx context.Context, req schema.QueueListRequest) (*schema.QueueList, error) {
	var list schema.QueueList
	if err := manager.conn.List(ctx, &list, req); err != nil {
		return nil, err
	}
	return &list, nil
}

// GetQueue returns a queue by name
func (manager *Manager) GetQueue(ctx context.Context, name string) (*schema.Queue, error) {
	var queue schema.Queue
	if err := manager.conn.Get(ctx, &queue, schema.QueueName(name)); err != nil {
		return nil, err
	}
	return &queue, nil
}

// DeleteQueue deletes an existing queue, and returns it
func (manager *Manager) DeleteQueue(ctx context.Context, name string) (*schema.Queue, error) {
	var queue schema.Queue
	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		return conn.Delete(ctx, &queue, schema.QueueName(name))
	}); err != nil {
		return nil, err
	}
	return &queue, nil
}

// UpdateQueue updates an existing queue, and returns it.
func (manager *Manager) UpdateQueue(ctx context.Context, name string, meta schema.QueueMeta) (*schema.Queue, error) {
	var queue schema.Queue
	if err := manager.conn.Update(ctx, &queue, schema.QueueName(name), meta); err != nil {
		return nil, err
	}
	return &queue, nil
}

// CleanQueue removes stale tasks from a queue, and returns the tasks removed
func (manager *Manager) CleanQueue(ctx context.Context, name string) ([]schema.Task, error) {
	var resp schema.QueueCleanResponse
	if err := manager.conn.List(ctx, &resp, schema.QueueCleanRequest{Queue: name}); err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// ListQueueStatuses returns the status breakdown for all queues in the namespace
func (manager *Manager) ListQueueStatuses(ctx context.Context) ([]schema.QueueStatus, error) {
	var resp schema.QueueStatusResponse
	if err := manager.conn.List(ctx, &resp, schema.QueueStatusRequest{}); err != nil {
		return nil, err
	}
	return resp.Body, nil
}
