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

func (c *Client) ListTablespaces(ctx context.Context, req schema.TablespaceListRequest) (*schema.TablespaceList, error) {
	var response schema.TablespaceList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("tablespace"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) GetTablespace(ctx context.Context, name string) (*schema.Tablespace, error) {
	var response schema.Tablespace
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("tablespace", name)); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) CreateTablespace(ctx context.Context, tablespace schema.TablespaceMeta, location string) (*schema.Tablespace, error) {
	req := struct {
		schema.TablespaceMeta
		Location string `json:"location" validate:"required" help:"Location for the tablespace"`
	}{
		TablespaceMeta: tablespace,
		Location:       location,
	}
	jsonReq, err := client.NewJSONRequest(req)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Tablespace
	if err := c.DoWithContext(ctx, jsonReq, &response, client.OptPath("tablespace")); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) DeleteTablespace(ctx context.Context, name string) error {
	return c.DoWithContext(ctx, client.MethodDelete, nil, client.OptPath("tablespace", name))
}

func (c *Client) UpdateTablespace(ctx context.Context, name string, meta schema.TablespaceMeta) (*schema.Tablespace, error) {
	req, err := client.NewJSONRequestEx(http.MethodPatch, meta, "")
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Tablespace
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("tablespace", name)); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}
