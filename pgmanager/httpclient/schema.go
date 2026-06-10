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

func (c *Client) ListSchemas(ctx context.Context, req schema.SchemaListRequest) (*schema.SchemaList, error) {
	// Set query parameters
	path := client.OptPath("schema")
	if req.Database != nil {
		path = client.OptPath("schema", types.Value(req.Database))
	}

	// Perform request
	var response schema.SchemaList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, path, client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}
