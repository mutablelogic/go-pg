package httpclient

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListTasks returns a page of tasks, oldest first, with the total number of
// tasks matching the request, which can have a status
func (c *Client) ListTasks(ctx context.Context, req schema.TaskListRequest) (*schema.TaskList, error) {
	var response schema.TaskList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("task"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}
