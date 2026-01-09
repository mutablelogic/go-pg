package main

import (
	"encoding/json"
	"fmt"
	"time"

	// Packages
	httpclient "github.com/mutablelogic/go-pg/pkg/queue/httpclient"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type TaskCommands struct {
	Tasks        ListTasksCommand    `cmd:"" name:"tasks" help:"List tasks with optional filters." group:"TASK"`
	CreateTask   CreateTaskCommand   `cmd:"" name:"create-task" help:"Create task." group:"TASK"`
	RetainTask   RetainTaskCommand   `cmd:"" name:"retain-task" help:"Retain (lock) next available task from queue." group:"TASK"`
	CompleteTask CompleteTaskCommand `cmd:"" name:"complete-task" help:"Mark task as completed or failed." group:"TASK"`
}

type ListTasksCommand struct {
	Queue  string `name:"queue" help:"Filter by queue name"`
	Status string `name:"status" help:"Filter by status (pending, retained, completed, failed)"`
	Offset uint64 `name:"offset" help:"Pagination offset" default:"0"`
	Limit  uint64 `name:"limit" help:"Pagination limit" default:"100"`
}

type CreateTaskCommand struct {
	Queue     string     `arg:"" name:"queue" help:"Queue name"`
	Payload   string     `name:"payload" help:"Task payload (JSON)"`
	DelayedAt *time.Time `name:"delayed-at" help:"Delay task until this time"`
}

type RetainTaskCommand struct {
	Queue  string `arg:"" name:"queue" help:"Queue name"`
	Worker string `arg:"" name:"worker" help:"Worker identifier"`
}

type CompleteTaskCommand struct {
	Id    uint64 `arg:"" name:"id" help:"Task ID"`
	Error string `name:"error" help:"Error payload (JSON) - marks task as failed"`
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *ListTasksCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Build options
	opts := []httpclient.Opt{
		httpclient.WithQueue(cmd.Queue),
		httpclient.WithStatus(cmd.Status),
		httpclient.WithOffset(cmd.Offset),
		httpclient.WithLimit(cmd.Limit),
	}

	// List tasks
	tasks, err := client.ListTasks(ctx.ctx, opts...)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(tasks)
	return nil
}

func (cmd *CreateTaskCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Parse payload
	var payload any
	if cmd.Payload != "" {
		if err := json.Unmarshal([]byte(cmd.Payload), &payload); err != nil {
			return fmt.Errorf("invalid payload JSON: %w", err)
		}
	}

	// Create task
	task, err := client.CreateTask(ctx.ctx, cmd.Queue, schema.TaskMeta{
		Payload:   payload,
		DelayedAt: cmd.DelayedAt,
	})
	if err != nil {
		return err
	}

	// Print
	fmt.Println(task)
	return nil
}

func (cmd *RetainTaskCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Retain task
	task, err := client.RetainTask(ctx.ctx, cmd.Queue, cmd.Worker)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(task)
	return nil
}

func (cmd *CompleteTaskCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Parse error JSON if provided
	var errPayload any
	if cmd.Error != "" {
		if err := json.Unmarshal([]byte(cmd.Error), &errPayload); err != nil {
			return fmt.Errorf("invalid error JSON: %w", err)
		}
	}

	// Complete/fail task
	task, err := client.ReleaseTask(ctx.ctx, cmd.Id, errPayload)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(task)
	return nil
}
