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

func (c *Client) ListExtensions(ctx context.Context, req schema.ExtensionListRequest) (*schema.ExtensionList, error) {
	// Perform request
	var response schema.ExtensionList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("extension"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}
