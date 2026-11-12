package cmd

import (
	"context"
	"fmt"
	"os"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	httpclient "github.com/mutablelogic/go-pg/pgqueue/httpclient"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	server "github.com/mutablelogic/go-server"
	tui "github.com/mutablelogic/go-server/pkg/tui"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type TaskCommands struct {
	TaskList     TaskListCmd     `cmd:"" name:"tasks" help:"List the tasks, optionally in some queues, or with some statuses." group:"TASK QUEUES"`
	TaskGet      TaskGetCmd      `cmd:"" name:"task" help:"Get a task, with its status, without retaining it." group:"TASK QUEUES"`
	TaskPriority TaskPriorityCmd `cmd:"" name:"task-priority" help:"Change the priority of a task which hasn't run yet." group:"TASK QUEUES"`
}

type TaskListCmd struct {
	pg.OffsetLimit `embed:""`
	Queue          []string `name:"queue" help:"Only list the tasks in these queues." optional:""`
	Status         []string `name:"status" help:"Only list the tasks with these statuses: new, delayed, running, retry, failed or done." optional:""`
}

type TaskGetCmd struct {
	Id uint64 `arg:"" help:"Task id"`
}

type TaskPriorityCmd struct {
	Id       uint64 `arg:"" help:"Task id"`
	Priority uint16 `arg:"" help:"Priority, where 0 is the highest, and lower runs first"`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *TaskListCmd) Run(ctx server.Cmd) error {
	// Width of the terminal
	width := ctx.IsTerm()
	debug := ctx.IsDebug()

	// Perform the request
	return WithClient(ctx, "pgqueue.TaskListCmd", func(ctx context.Context, client *httpclient.Client) error {
		req := schema.TaskListRequest{OffsetLimit: cmd.OffsetLimit, Queue: cmd.Queue, Status: cmd.Status}
		list, err := client.ListTasks(ctx, req)
		if err != nil {
			return err
		}

		// With debugging
		if debug {
			fmt.Println(list)
			return nil
		}

		// List table
		table := tui.TableFor[schema.TaskWithStatus](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, list.Body...); err != nil {
			return err
		}

		// List summary
		summary := tui.TableSummary("tasks", uint(list.Count), uint(len(list.Body)), list.Offset, list.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

func (cmd *TaskGetCmd) Run(ctx server.Cmd) error {
	return WithClient(ctx, "pgqueue.TaskGetCmd", func(ctx context.Context, client *httpclient.Client) error {
		task, err := client.GetTask(ctx, cmd.Id)
		if err != nil {
			return err
		}

		fmt.Println(task)
		return nil
	})
}

func (cmd *TaskPriorityCmd) Run(ctx server.Cmd) error {
	return WithClient(ctx, "pgqueue.TaskPriorityCmd", func(ctx context.Context, client *httpclient.Client) error {
		task, err := client.UpdateTask(ctx, cmd.Id, schema.TaskUpdate{Priority: &cmd.Priority})
		if err != nil {
			return err
		}

		fmt.Println(task)
		return nil
	})
}
