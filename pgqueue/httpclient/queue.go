package httpclient

import (
	"context"

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
