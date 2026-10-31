package cmd

import (
	"context"
	"encoding/json"
	"os"

	// Packages
	httpclient "github.com/mutablelogic/go-pg/pgbroadcaster/httpclient"
	schema "github.com/mutablelogic/go-pg/pgbroadcaster/schema"
	server "github.com/mutablelogic/go-server"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type EventCommands struct {
	Events EventsCmd `cmd:"" name:"events" help:"Watch the server's events, which are the changes to its tables, until interrupted." group:"EVENTS"`
}

type EventsCmd struct {
	schema.EventFilter `embed:""`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Run prints each event as a line of JSON, until the command is interrupted
func (cmd *EventsCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "pgbroadcaster.EventsCmd", func(ctx context.Context, client *httpclient.Client) error {
		encoder := json.NewEncoder(os.Stdout)
		return client.WatchEvents(ctx, cmd.EventFilter, func(event schema.Event) error {
			return encoder.Encode(event)
		})
	})
}
