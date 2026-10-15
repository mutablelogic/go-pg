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
	QueueList QueueListCmd `cmd:"" name:"queues" help:"List the queues." group:"TASK QUEUES"`
}

type QueueListCmd struct {
	schema.QueueListRequest
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
		table := tui.TableFor[schema.Queue](tui.SetWidth(width))
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
