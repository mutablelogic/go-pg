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

func (c *Client) ListSettings(ctx context.Context, opts ...Opt) (*schema.SettingList, error) {
	req := client.NewRequest()

	// Apply options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.SettingList
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("setting"), client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

func (c *Client) ListSettingCategories(ctx context.Context) (*schema.SettingCategoryList, error) {
	req := client.NewRequest()

	// Perform request
	var response schema.SettingCategoryList
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("setting", "category")); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

func (c *Client) GetSetting(ctx context.Context, name string) (*schema.Setting, error) {
	req := client.NewRequest()

	// Perform request
	var response schema.Setting
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("setting", name)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

func (c *Client) UpdateSetting(ctx context.Context, name string, meta schema.SettingMeta, opts ...Opt) (*schema.Setting, error) {
	req, err := client.NewJSONRequestEx(http.MethodPatch, meta, "")
	if err != nil {
		return nil, err
	}

	// Apply options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Setting
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("setting", name), client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}
