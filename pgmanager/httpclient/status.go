package httpclient

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-client"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) Ping(ctx context.Context) error {
	req := client.NewRequest()
	if err := c.DoWithContext(ctx, req, nil, client.OptPath("health")); err != nil {
		return err
	}

	// Return success
	return nil
}
