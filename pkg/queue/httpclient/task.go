package httpclient

import (
	"context"
	"fmt"
	"net/http"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListTasks returns all tasks with optional filtering by status (GET /task).
func (c *Client) ListTasks(ctx context.Context, opts ...Opt) (*schema.TaskList, error) {
	req := client.NewRequest()

	// Apply options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.TaskList
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("task"), client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

// RetainTask gets the next available task from a queue (GET /task/{queue}?worker=XX).
func (c *Client) RetainTask(ctx context.Context, queue, worker string) (*schema.TaskWithStatus, error) {
	req := client.NewRequest()

	// Apply options
	opts := []Opt{WithWorker(worker)}
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.TaskWithStatus
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("task", queue), client.OptQuery(opt.Values)); err != nil {
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

// ReleaseTask releases a task (PATCH /task/{id}).
// If result is non-nil, marks the task as failed with result as the error payload.
// If result is nil, marks the task as completed successfully.
func (c *Client) ReleaseTask(ctx context.Context, id uint64, result any) (*schema.TaskWithStatus, error) {
	payload := struct {
		Result any  `json:"result,omitempty"`
		Fail   bool `json:"fail,omitempty"`
	}{
		Result: result,
		Fail:   result != nil,
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
