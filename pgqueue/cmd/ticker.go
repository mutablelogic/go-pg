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

type TickerCommands struct {
	TickerList TickerListCmd `cmd:"" name:"tickers" help:"List the tickers." group:"TASK QUEUES"`
}

type TickerListCmd struct {
	schema.TickerListRequest
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *TickerListCmd) Run(ctx server.Cmd) error {
	// Width of the terminal
	width := ctx.IsTerm()
	debug := ctx.IsDebug()

	// Perform the request
	return WithClient(ctx, "pgqueue.TickerListCmd", func(ctx context.Context, client *httpclient.Client) error {
		list, err := client.ListTickers(ctx, cmd.TickerListRequest)
		if err != nil {
			return err
		}

		// With debugging
		if debug {
			fmt.Println(list)
			return nil
		}

		// List table
		table := tui.TableFor[schema.Ticker](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, list.Body...); err != nil {
			return err
		}

		// List summary
		summary := tui.TableSummary("tickers", uint(list.Count), uint(len(list.Body)), list.Offset, list.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}
