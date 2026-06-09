package httpclient

import (
	"context"
	"net/http"

	// Packages
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	client "github.com/mutablelogic/go-client"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) ListExtensions(ctx context.Context, opts ...Opt) (*schema.ExtensionList, error) {
	req := client.NewRequest()

	// Apply options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.ExtensionList
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("extension"), client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

func (c *Client) GetExtension(ctx context.Context, name string) (*schema.Extension, error) {
	req := client.NewRequest()

	// Perform request
	var response schema.Extension
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("extension", name)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

func (c *Client) CreateExtension(ctx context.Context, meta schema.ExtensionMeta, opts ...Opt) (*schema.Extension, error) {
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	req, err := client.NewJSONRequest(meta)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Extension
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("extension"), client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

func (c *Client) DeleteExtension(ctx context.Context, name string, opts ...Opt) error {
	opt, err := applyOpts(opts...)
	if err != nil {
		return err
	}
	return c.DoWithContext(ctx, client.MethodDelete, nil, client.OptPath("extension", name), client.OptQuery(opt.Values))
}

func (c *Client) UpdateExtension(ctx context.Context, name string, meta schema.ExtensionMeta) (*schema.Extension, error) {
	req, err := client.NewJSONRequestEx(http.MethodPatch, meta, "")
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Extension
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("extension", name)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}
