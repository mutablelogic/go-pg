package httpclient

import (
	"context"
	"net/url"

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

func (c *Client) GetExtension(ctx context.Context, name string) (*schema.Extension, error) {
	req := client.NewRequest()

	// Perform request
	var response schema.Extension
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("extension", name)); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) CreateExtension(ctx context.Context, meta schema.ExtensionMeta, cascade bool) (*schema.Extension, error) {
	req, err := client.NewJSONRequest(meta)
	if err != nil {
		return nil, err
	}

	// cascade value
	query := url.Values{}
	if cascade {
		query.Set("cascade", "true")
	}

	// Perform request
	var response schema.Extension
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("extension"), client.OptQuery(query)); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}
