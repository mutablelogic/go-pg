package httpclient
package httpclient

import (
	"context"
	"fmt"
	"net/http"

	// Packages
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	client "github.com/mutablelogic/go-client"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RetainTask gets the next available task from a queue (GET /task).
func (c *Client) RetainTask(ctx context.Context, queue, worker string) (*schema.TaskWithStatus, error) {
	req := client.NewRequest()

	// Apply options
	opts := []Opt{WithQueue(queue), WithWorker(worker)}
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.TaskWithStatus
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("task"), client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

// CreateTask creates a new task in a queue (POST /task).
func (c *Client) CreateTask(ctx context.Context, queue string, meta schema.TaskMeta) (*schema.Task, error) {
	payload := struct {
		Queue string `json:"queue"`
		schema.TaskMeta
	}{
		Queue:    queue,
		TaskMeta: meta,
	}

	req, err := client.NewJSONRequest(payload)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Task
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("task")); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

// ReleaseTask releases a task with a result (PATCH /task/{id}).
func (c *Client) ReleaseTask(ctx context.Context, id uint64, result any) (*schema.TaskWithStatus, error) {
	payload := struct {
		Result any `json:"result,omitempty"`
	}{
		Result: result,
	}

	req, err := client.NewJSONRequestEx(http.MethodPatch, payload, "")
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.TaskWithStatus
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("task", fmt.Sprint(id))); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

// CompleteTask marks a task as successfully completed (DELETE /task/{id}).
func (c *Client) CompleteTask(ctx context.Context, id uint64) (*schema.TaskWithStatus, error) {
	req := client.NewRequestEx(http.MethodDelete, "")

	// Perform request
	var response schema.TaskWithStatus
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("task", fmt.Sprint(id))); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}
