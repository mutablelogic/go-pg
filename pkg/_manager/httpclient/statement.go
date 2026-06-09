package httpclient

import (
	"context"

	// Packages
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	client "github.com/mutablelogic/go-client"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListStatements returns statement statistics from pg_stat_statements.
// Supports filtering by database, user, and ordering.
func (c *Client) ListStatements(ctx context.Context, opts ...Opt) (*schema.StatementList, error) {
	req := client.NewRequest()

	// Apply options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.StatementList
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("statement"), client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

// ResetStatements resets all statement statistics.
func (c *Client) ResetStatements(ctx context.Context) error {
	return c.DoWithContext(ctx, client.MethodDelete, nil, client.OptPath("statement"))
}
