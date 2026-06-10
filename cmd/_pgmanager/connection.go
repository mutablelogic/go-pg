package main

import (
	"fmt"

	// Packages
	httpclient "github.com/mutablelogic/go-pg/pkg/manager/httpclient"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ConnectionCommands struct {
	ListConnection   ListConnectionCommand   `cmd:"" name:"connections" help:"List connections."`
	GetConnection    GetConnectionCommand    `cmd:"" name:"connection" help:"Get connection."`
	DeleteConnection DeleteConnectionCommand `cmd:"" name:"delete-connection" help:"Delete (terminate) connection."`
}

type ListConnectionCommand struct {
	Database string `name:"database" help:"Filter by database name"`
	Role     string `name:"role" help:"Filter by role name"`
	State    string `name:"state" help:"Filter by state (active, idle, etc.)"`
}

type GetConnectionCommand struct {
	Pid uint64 `arg:"" name:"pid" help:"Process ID"`
}

type DeleteConnectionCommand struct {
	GetConnectionCommand
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *ListConnectionCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Build options
	opts := []httpclient.Opt{}
	if cmd.Database != "" {
		opts = append(opts, httpclient.OptDatabase(cmd.Database))
	}
	if cmd.Role != "" {
		opts = append(opts, httpclient.OptRole(cmd.Role))
	}
	if cmd.State != "" {
		opts = append(opts, httpclient.OptState(cmd.State))
	}

	// List connections
	connections, err := client.ListConnections(ctx.ctx, opts...)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(connections)
	return nil
}

func (cmd *GetConnectionCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Get one connection
	connection, err := client.GetConnection(ctx.ctx, cmd.Pid)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(connection)
	return nil
}

func (cmd *DeleteConnectionCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Delete (terminate) connection
	if err := client.DeleteConnection(ctx.ctx, cmd.Pid); err != nil {
		return err
	}

	// Return success
	return nil
}
