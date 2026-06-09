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

// ListSchemas returns a list of schemas. If database is non-empty,
// only schemas from that database are returned.
func (c *Client) ListSchemas(ctx context.Context, database string, opts ...Opt) (*schema.SchemaList, error) {
	req := client.NewRequest()

	// Apply options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Build path based on whether database is specified
	var pathOpt client.RequestOpt
	if database != "" {
		pathOpt = client.OptPath("schema", database)
	} else {
		pathOpt = client.OptPath("schema")
	}

	// Perform request
	var response schema.SchemaList
	if err := c.DoWithContext(ctx, req, &response, pathOpt, client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

// GetSchema returns a schema by database and namespace name.
func (c *Client) GetSchema(ctx context.Context, database, namespace string) (*schema.Schema, error) {
	req := client.NewRequest()

	// Perform request
	var response schema.Schema
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("schema", database, namespace)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

// CreateSchema creates a new schema in the specified database.
func (c *Client) CreateSchema(ctx context.Context, database string, meta schema.SchemaMeta) (*schema.Schema, error) {
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
	return &response, nil
}

// DeleteSchema deletes a schema by database and namespace name.
func (c *Client) DeleteSchema(ctx context.Context, database, namespace string, opt ...Opt) error {
	opts, err := applyOpts(opt...)
	if err != nil {
		return err
	}
	return c.DoWithContext(ctx, client.MethodDelete, nil, client.OptPath("schema", database, namespace), client.OptQuery(opts.Values))
}

// UpdateSchema updates a schema by database and namespace name.
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
	return &response, nil
}
