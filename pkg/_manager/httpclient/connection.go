package httpclient

import (
	"context"

	// Packages
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	client "github.com/mutablelogic/go-client"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) ListConnections(ctx context.Context, opts ...Opt) (*schema.ConnectionList, error) {
	req := client.NewRequest()

	// Apply options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.ConnectionList
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("connection"), client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

func (c *Client) GetConnection(ctx context.Context, pid uint64) (*schema.Connection, error) {
	req := client.NewRequest()

	// Perform request
	var response schema.Connection
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("connection", pid)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

func (c *Client) DeleteConnection(ctx context.Context, pid uint64) error {
	return c.DoWithContext(ctx, client.MethodDelete, nil, client.OptPath("connection", pid))
}
