package httpclient

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) ListStatements(ctx context.Context, req schema.StatementListRequest) (*schema.StatementList, error) {
	var response schema.StatementList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("statement"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) ResetStatementStats(ctx context.Context) error {
	return c.DoWithContext(ctx, client.MethodDelete, nil, client.OptPath("statement"))
}
