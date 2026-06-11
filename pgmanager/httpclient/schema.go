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

func (c *Client) ListSchemas(ctx context.Context, req schema.SchemaListRequest) (*schema.SchemaList, error) {
	// Set query parameters
	path := client.OptPath("schema")
	if req.Database != nil {
		path = client.OptPath("schema", types.Value(req.Database))
	}

	// Perform request
	var response schema.SchemaList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, path, client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) GetSchema(ctx context.Context, database, namespace string) (*schema.Schema, error) {
	// Perform request
	var response schema.Schema
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("schema", database, namespace)); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) CreateSchema(ctx context.Context, database string, meta schema.SchemaMeta) (*schema.Schema, error) {
	// Create JSON request body
	req, err := client.NewJSONRequest(meta)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Schema
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("schema", database)); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) DeleteSchema(ctx context.Context, database, namespace string, force bool) (*schema.Schema, error) {
	query := url.Values{}
	if force {
		query.Set("force", "true")
	}
	var response schema.Schema
	if err := c.DoWithContext(ctx, client.MethodDelete, &response, client.OptPath("schema", database, namespace), client.OptQuery(query)); err != nil {
		return nil, err
	}
	return types.Ptr(response), nil
}

func (c *Client) UpdateSchema(ctx context.Context, database, namespace string, meta schema.SchemaMeta) (*schema.Schema, error) {
	req, err := client.NewJSONRequestEx(http.MethodPatch, meta, "")
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Schema
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("schema", database, namespace)); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}
