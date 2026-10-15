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
	TaskList TaskListCmd `cmd:"" name:"tasks" help:"List the tasks, optionally in some queues, or with a status." group:"TASK QUEUES"`
}

type TaskListCmd struct {
	pg.OffsetLimit `embed:""`
	Queue          []string `name:"queue" help:"Only list the tasks in these queues." optional:""`
	Status         string   `name:"status" help:"Only list the tasks with this status." enum:"any,new,running,retry,expired,failed,done" default:"any"`
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// statusAny is the status which lists the tasks with any status, which isn't
// sent to the server, as it lists the tasks with any status without one
const statusAny = "any"

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *TaskListCmd) Run(ctx server.Cmd) error {
	// Width of the terminal
	width := ctx.IsTerm()
	debug := ctx.IsDebug()

	// Perform the request
	return WithClient(ctx, "pgqueue.TaskListCmd", func(ctx context.Context, client *httpclient.Client) error {
		req := schema.TaskListRequest{OffsetLimit: cmd.OffsetLimit, Queue: cmd.Queue, Status: cmd.Status}
		if req.Status == statusAny {
			req.Status = ""
		}
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
