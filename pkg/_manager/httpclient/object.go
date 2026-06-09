package httpclient

import (
	"context"

	// Packages
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	client "github.com/mutablelogic/go-client"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListObjects returns a list of objects. If database is non-empty,
// only objects from that database are returned. If namespace is also non-empty,
// objects are further filtered by schema.
func (c *Client) ListObjects(ctx context.Context, database, namespace string, opts ...Opt) (*schema.ObjectList, error) {
	req := client.NewRequest()

	// Apply options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Build path based on whether database/namespace is specified
	var pathOpt client.RequestOpt
	switch {
	case database != "" && namespace != "":
		pathOpt = client.OptPath("object", database, namespace)
	case database != "":
		pathOpt = client.OptPath("object", database)
	default:
		pathOpt = client.OptPath("object")
	}

	// Perform request
	var response schema.ObjectList
	if err := c.DoWithContext(ctx, req, &response, pathOpt, client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

// GetObject returns an object by database, namespace (schema), and name.
func (c *Client) GetObject(ctx context.Context, database, namespace, name string) (*schema.Object, error) {
	req := client.NewRequest()

	// Perform request
	var response schema.Object
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("object", database, namespace, name)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}
