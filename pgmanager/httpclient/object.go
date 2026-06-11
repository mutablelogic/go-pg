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

func (c *Client) ListObjects(ctx context.Context, req schema.ObjectListRequest) (*schema.ObjectList, error) {
	var response schema.ObjectList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("object"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) GetObject(ctx context.Context, database, namespace, name string) (*schema.Object, error) {
	var response schema.Object
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("object", database, namespace, name)); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}
