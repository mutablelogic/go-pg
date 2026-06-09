package httpclient

import (
	"context"
	"net/http"
	"net/url"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) ListDatabases(ctx context.Context, req schema.DatabaseListRequest) (*schema.DatabaseList, error) {
	// Perform request
	var response schema.DatabaseList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("database"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) GetDatabase(ctx context.Context, name string) (*schema.Database, error) {
	req := client.NewRequest()

	// Perform request
	var response schema.Database
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("database", name)); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) CreateDatabase(ctx context.Context, database schema.DatabaseMeta) (*schema.Database, error) {
	req, err := client.NewJSONRequest(database)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Database
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("database")); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) DeleteDatabase(ctx context.Context, name string, force bool) error {
	query := url.Values{}
	if force {
		query.Set("force", "true")
	}
	return c.DoWithContext(ctx, client.MethodDelete, nil, client.OptPath("database", name), client.OptQuery(query))
}

func (c *Client) UpdateDatabase(ctx context.Context, name string, meta schema.DatabaseMeta) (*schema.Database, error) {
	req, err := client.NewJSONRequestEx(http.MethodPatch, meta, "")
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Database
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("database", name)); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}
