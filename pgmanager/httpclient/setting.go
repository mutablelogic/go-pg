package httpclient

import (
	"context"
	"net/http"

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

func (c *Client) GetSetting(ctx context.Context, name string) (*schema.Setting, error) {
	var response schema.Setting
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("setting", name)); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) UpdateSetting(ctx context.Context, name string, meta schema.SettingMeta) (*schema.Setting, error) {
	req, err := client.NewJSONRequestEx(http.MethodPatch, meta, "")
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Setting
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("setting", name)); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}
