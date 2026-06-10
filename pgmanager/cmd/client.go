package cmd

import (
	"context"
	"fmt"
	"os"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	httpclient "github.com/mutablelogic/go-pg/pgmanager/httpclient"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	server "github.com/mutablelogic/go-server"
	tui "github.com/mutablelogic/go-server/pkg/tui"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ClientCommands struct {
	Ping PingCmd `cmd:"" name:"ping" help:"Ping the server." group:"STATUS"`
	DatabaseClientCommands
	ConnectionClientCommands
	ExtensionClientCommands
}

type PingCmd struct{}

type DatabaseClientCommands struct {
	DatabaseList   DatabaseListCmd   `cmd:"" name:"databases" help:"List databases." group:"DATABASE"`
	DatabaseGet    DatabaseGetCmd    `cmd:"" name:"database" help:"Get database details." group:"DATABASE"`
	DatabaseCreate DatabaseCreateCmd `cmd:"" name:"database-create" help:"Create a new database." group:"DATABASE"`
	DatabaseDelete DatabaseDeleteCmd `cmd:"" name:"database-delete" help:"Delete a database." group:"DATABASE"`
	DatabaseUpdate DatabaseUpdateCmd `cmd:"" name:"database-update" help:"Update a database." group:"DATABASE"`
}

type ConnectionClientCommands struct {
	ConnectionList   ConnectionListCmd   `cmd:"" name:"connections" help:"List connections." group:"CONNECTION"`
	ConnectionGet    ConnectionGetCmd    `cmd:"" name:"connection" help:"Get connection details." group:"CONNECTION"`
	ConnectionDelete ConnectionDeleteCmd `cmd:"" name:"connection-delete" help:"Delete a connection." group:"CONNECTION"`
}

type ExtensionClientCommands struct {
	ExtensionList ExtensionListCmd `cmd:"" name:"extensions" help:"List extensions." group:"EXTENSION"`
}

type DatabaseListCmd struct {
	schema.DatabaseListRequest
}

type DatabaseGetCmd struct {
	Name string `arg:"" name:"name" help:"Name of the database."`
}

type DatabaseCreateCmd struct {
	schema.DatabaseMeta
}

type DatabaseDeleteCmd struct {
	Name string `arg:"" name:"name" help:"Name of the database."`
}

type DatabaseUpdateCmd struct {
	NewName string `flag:"" name:"name" help:"New name of the database."`
	schema.DatabaseMeta
}

type ConnectionListCmd struct {
	schema.ConnectionListRequest
}

type ConnectionGetCmd struct {
	Pid uint64 `arg:"" name:"pid" help:"PID of the connection."`
}

type ConnectionDeleteCmd struct {
	Pid uint64 `arg:"" name:"pid" help:"PID of the connection."`
}

type ExtensionListCmd struct {
	schema.ExtensionListRequest
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func withClient(ctx server.Cmd, span string, fn func(context.Context, *httpclient.Client) error) error {
	endpoint, opts, err := ctx.ClientEndpoint()
	if err != nil {
		return err
	} else if client, err := httpclient.New(endpoint, opts...); err != nil {
		return err
	} else {
		var err error
		ctx, endfn := otel.StartSpan(ctx.Tracer(), ctx.Context(), span)
		defer func() { endfn(err) }()
		err = fn(ctx, client)
		return err
	}
}

///////////////////////////////////////////////////////////////////////////////
// DATABASE COMMANDS

func (cmd *PingCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "ping", func(ctx context.Context, client *httpclient.Client) error {
		return client.Ping(ctx)
	})
}

func (cmd *DatabaseListCmd) Run(ctx server.Cmd) error {
	// Set the width of the terminal
	width := ctx.IsTerm()

	// Perform the request
	return withClient(ctx, "databases", func(ctx context.Context, client *httpclient.Client) error {
		databases, err := client.ListDatabases(ctx, cmd.DatabaseListRequest)
		if err != nil {
			return err
		}

		// Databases list table
		table := tui.TableFor[schema.Database](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, databases.Body...); err != nil {
			return err
		}

		// Databases list summary
		summary := tui.TableSummary("databases", uint(databases.Count), databases.Offset, databases.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

func (cmd *DatabaseGetCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "database", func(ctx context.Context, client *httpclient.Client) error {
		database, err := client.GetDatabase(ctx, cmd.Name)
		if err != nil {
			return err
		}

		fmt.Println(database)
		return nil
	})
}

func (cmd *DatabaseCreateCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "database-create", func(ctx context.Context, client *httpclient.Client) error {
		database, err := client.CreateDatabase(ctx, cmd.DatabaseMeta)
		if err != nil {
			return err
		}

		fmt.Println(database)
		return nil
	})
}

func (cmd *DatabaseDeleteCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "database-delete", func(ctx context.Context, client *httpclient.Client) error {
		return client.DeleteDatabase(ctx, cmd.Name, false)
	})
}

func (cmd *DatabaseUpdateCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "database-update", func(ctx context.Context, client *httpclient.Client) error {
		// We swap the name in the meta with the new name
		cmd.Name, cmd.NewName = cmd.NewName, cmd.Name

		// Perform the update
		database, err := client.UpdateDatabase(ctx, cmd.NewName, cmd.DatabaseMeta)
		if err != nil {
			return err
		}

		fmt.Println(database)
		return nil
	})
}

///////////////////////////////////////////////////////////////////////////////
// CONNECTION COMMANDS

func (cmd *ConnectionListCmd) Run(ctx server.Cmd) error {
	// Set the width of the terminal
	width := ctx.IsTerm()

	// Perform the request
	return withClient(ctx, "connections", func(ctx context.Context, client *httpclient.Client) error {
		connections, err := client.ListConnections(ctx, cmd.ConnectionListRequest)
		if err != nil {
			return err
		}

		// Connections list table
		table := tui.TableFor[schema.Connection](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, connections.Body...); err != nil {
			return err
		}

		// Connections list summary
		summary := tui.TableSummary("connections", uint(connections.Count), connections.Offset, connections.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

func (cmd *ConnectionGetCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "connection", func(ctx context.Context, client *httpclient.Client) error {
		connection, err := client.GetConnection(ctx, cmd.Pid)
		if err != nil {
			return err
		}

		fmt.Println(connection)
		return nil
	})
}

func (cmd *ConnectionDeleteCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "connection-delete", func(ctx context.Context, client *httpclient.Client) error {
		return client.DeleteConnection(ctx, cmd.Pid)
	})
}

///////////////////////////////////////////////////////////////////////////////
// EXTENSION COMMANDS

func (cmd *ExtensionListCmd) Run(ctx server.Cmd) error {
	// Set the width of the terminal
	width := ctx.IsTerm()

	// Perform the request
	return withClient(ctx, "extensions", func(ctx context.Context, client *httpclient.Client) error {
		extensions, err := client.ListExtensions(ctx, cmd.ExtensionListRequest)
		if err != nil {
			return err
		}

		// Extensions list table
		table := tui.TableFor[schema.Extension](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, extensions.Body...); err != nil {
			return err
		}

		// Extensions list summary
		summary := tui.TableSummary("extensions", uint(extensions.Count), extensions.Offset, extensions.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}
