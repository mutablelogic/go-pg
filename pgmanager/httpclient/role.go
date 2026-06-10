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

func (c *Client) ListRoles(ctx context.Context, req schema.RoleListRequest) (*schema.RoleList, error) {
	var response schema.RoleList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("role"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) CreateRole(ctx context.Context, meta schema.RoleMeta) (*schema.Role, error) {
	req, err := client.NewJSONRequestEx(http.MethodPost, meta, "")
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Role
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("role")); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}
