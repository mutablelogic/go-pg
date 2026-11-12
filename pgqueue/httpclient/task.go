package httpclient

import (
	"context"
	"net/http"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListTasks returns a page of tasks, by priority, highest first, which is the
// lowest, and then oldest first, with the total number of
// tasks matching the request, which can have a status
func (c *Client) ListTasks(ctx context.Context, req schema.TaskListRequest) (*schema.TaskList, error) {
	var response schema.TaskList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("task"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}

// GetTask returns a task by id, with its status. The task isn't retained
// or changed
func (c *Client) GetTask(ctx context.Context, id uint64) (*schema.TaskWithStatus, error) {
	var response schema.TaskWithStatus
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("task", id)); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}

// UpdateTask changes the priority of a task which hasn't run yet, so is new,
// delayed, or waiting to be retried, and returns it, with its status
func (c *Client) UpdateTask(ctx context.Context, id uint64, req schema.TaskUpdate) (*schema.TaskWithStatus, error) {
	payload, err := client.NewJSONRequestEx(http.MethodPatch, req, "")
	if err != nil {
		return nil, err
	}

	var response schema.TaskWithStatus
	if err := c.DoWithContext(ctx, payload, &response, client.OptPath("task", id)); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}
