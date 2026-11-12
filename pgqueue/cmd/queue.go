package cmd

import (
	"context"
	"fmt"
	"os"

	// Packages
	httpclient "github.com/mutablelogic/go-pg/pgqueue/httpclient"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	server "github.com/mutablelogic/go-server"
	tui "github.com/mutablelogic/go-server/pkg/tui"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type QueueCommands struct {
	QueueList   QueueListCmd   `cmd:"" name:"queues" help:"List the queues." group:"TASK QUEUES"`
	QueueGet    QueueGetCmd    `cmd:"" name:"queue" help:"Get a queue." group:"TASK QUEUES"`
	QueueUpdate QueueUpdateCmd `cmd:"" name:"queue-update" help:"Change a queue's settings." group:"TASK QUEUES"`
	QueueDelete QueueDeleteCmd `cmd:"" name:"queue-delete" help:"Delete a queue, with all its tasks." group:"TASK QUEUES"`
	QueueClean  QueueCleanCmd  `cmd:"" name:"queue-clean" help:"Delete a queue's done and failed tasks, after the retention period." group:"TASK QUEUES"`
}

type QueueListCmd struct {
	schema.QueueListRequest
}

type QueueGetCmd struct {
	Name string `arg:"" help:"Queue name"`
}

type QueueUpdateCmd struct {
	Name             string `arg:"" help:"Queue name"`
	schema.QueueMeta `embed:""`
}

type QueueDeleteCmd struct {
	Name string `arg:"" help:"Queue name"`
}

type QueueCleanCmd struct {
	Name string `arg:"" help:"Queue name"`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *QueueListCmd) Run(ctx server.Cmd) error {
	// Width of the terminal
	width := ctx.IsTerm()
	debug := ctx.IsDebug()

	// Perform the request
	return WithClient(ctx, "pgqueue.QueueListCmd", func(ctx context.Context, client *httpclient.Client) error {
		queues, err := client.ListQueues(ctx, cmd.QueueListRequest)
		if err != nil {
			return err
		}

		// With debugging
		if debug {
			fmt.Println(queues)
			return nil
		}

		// Queues list table
		table := tui.TableFor[schema.QueueWithStatus](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, queues.Body...); err != nil {
			return err
		}

		// Queues list summary
		summary := tui.TableSummary("queues", uint(queues.Count), uint(len(queues.Body)), queues.Offset, queues.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

func (cmd *QueueGetCmd) Run(ctx server.Cmd) error {
	return WithClient(ctx, "pgqueue.QueueGetCmd", func(ctx context.Context, client *httpclient.Client) error {
		queue, err := client.GetQueue(ctx, cmd.Name)
		if err != nil {
			return err
		}

		fmt.Println(queue)
		return nil
	})
}

func (cmd *QueueUpdateCmd) Run(ctx server.Cmd) error {
	return WithClient(ctx, "pgqueue.QueueUpdateCmd", func(ctx context.Context, client *httpclient.Client) error {
		queue, err := client.UpdateQueue(ctx, cmd.Name, cmd.QueueMeta)
		if err != nil {
			return err
		}

		fmt.Println(queue)
		return nil
	})
}

func (cmd *QueueDeleteCmd) Run(ctx server.Cmd) error {
	return WithClient(ctx, "pgqueue.QueueDeleteCmd", func(ctx context.Context, client *httpclient.Client) error {
		queue, err := client.DeleteQueue(ctx, cmd.Name)
		if err != nil {
			return err
		}

		fmt.Println(queue)
		return nil
	})
}

func (cmd *QueueCleanCmd) Run(ctx server.Cmd) error {
	return WithClient(ctx, "pgqueue.QueueCleanCmd", func(ctx context.Context, client *httpclient.Client) error {
		cleaned, err := client.CleanQueue(ctx, cmd.Name)
		if err != nil {
			return err
		}

		fmt.Println(cleaned)
		return nil
	})
}
