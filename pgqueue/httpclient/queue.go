package httpclient

import (
	"context"
	"net/http"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListQueues returns a page of queues, in name order, with the total number
// of queues
func (c *Client) ListQueues(ctx context.Context, req schema.QueueListRequest) (*schema.QueueList, error) {
	var response schema.QueueList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("queue"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}

// GetQueue returns a queue by name
func (c *Client) GetQueue(ctx context.Context, name string) (*schema.QueueWithStatus, error) {
	var response schema.QueueWithStatus
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("queue", name)); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}

// UpdateQueue changes a queue's settings, keeping those which aren't set,
// and returns the queue
func (c *Client) UpdateQueue(ctx context.Context, name string, meta schema.QueueMeta) (*schema.Queue, error) {
	req, err := client.NewJSONRequestEx(http.MethodPatch, meta, "")
	if err != nil {
		return nil, err
	}

	var response schema.Queue
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("queue", name)); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}

// DeleteQueue deletes a queue, with all its tasks, and returns it
func (c *Client) DeleteQueue(ctx context.Context, name string) (*schema.Queue, error) {
	var response schema.Queue
	if err := c.DoWithContext(ctx, client.MethodDelete, &response, client.OptPath("queue", name)); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}

// CleanQueue deletes up to 100 of a queue's done and failed tasks, which
// finished longer ago than the retention period, and returns them
func (c *Client) CleanQueue(ctx context.Context, name string) (*schema.QueueCleanResponse, error) {
	var response schema.QueueCleanResponse
	if err := c.DoWithContext(ctx, client.MethodPost, &response, client.OptPath("queue", name, "clean")); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}
