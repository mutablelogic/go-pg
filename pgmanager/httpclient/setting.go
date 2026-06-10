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

func (c *Client) ListSettings(ctx context.Context, req schema.SettingListRequest) (*schema.SettingList, error) {
	var response schema.SettingList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("setting"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) ListSettingCategories(ctx context.Context, req schema.SettingCategoryListRequest) (*schema.SettingCategoryList, error) {
	var response schema.SettingCategoryList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("setting", "category"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}
