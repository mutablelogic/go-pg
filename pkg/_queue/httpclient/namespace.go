package httpclient

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) ListNamespaces(ctx context.Context) (*schema.NamespaceList, error) {
	req := client.NewRequest()

	// Perform request
	var response schema.NamespaceList
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("namespace")); err != nil {
		return nil, err
	}

	// Return the response
	return &response, nil
}
