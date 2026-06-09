package main

import (
	"fmt"

	// Packages
	httpclient "github.com/mutablelogic/go-pg/pkg/manager/httpclient"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type DatabaseCommands struct {
	ListDatabase   ListDatabaseCommand   `cmd:"" name:"databases" help:"List databases."`
	GetDatabase    GetDatabaseCommand    `cmd:"" name:"database" help:"Get database."`
	CreateDatabase CreateDatabaseCommand `cmd:"" name:"create-database" help:"Create database."`
	DeleteDatabase DeleteDatabaseCommand `cmd:"" name:"delete-database" help:"Delete database."`
	UpdateDatabase UpdateDatabaseCommand `cmd:"" name:"update-database" help:"Update database."`
}

type ListDatabaseCommand struct {
	Offset uint64  `name:"offset" help:"Offset for pagination"`
	Limit  *uint64 `name:"limit" help:"Limit for pagination"`
}

type GetDatabaseCommand struct {
	Name string `arg:"" name:"name" help:"Database name"`
}

type DeleteDatabaseCommand struct {
	GetDatabaseCommand
}

type CreateDatabaseCommand struct {
	GetDatabaseCommand
	Owner string   `name:"owner" help:"Database owner"`
	Acl   []string `name:"acl" help:"Access control list entries (format: role:priv,priv,... e.g. myuser:SELECT,INSERT)"`
}

type UpdateDatabaseCommand struct {
	GetDatabaseCommand
	NewName string   `name:"rename" help:"Rename database to this name"`
	Owner   string   `name:"owner" help:"Database owner"`
	Acl     []string `name:"acl" help:"Access control list entries (format: role:priv,priv,... e.g. myuser:SELECT,INSERT)"`
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *ListDatabaseCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// List databases
	databases, err := client.ListDatabases(ctx.ctx, httpclient.WithOffsetLimit(cmd.Offset, cmd.Limit))
	if err != nil {
		return err
	}

	// Print
	fmt.Println(databases)
	return nil
}

func (cmd *GetDatabaseCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Get one database
	database, err := client.GetDatabase(ctx.ctx, cmd.Name)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(database)
	return nil
}

func (cmd *CreateDatabaseCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Parse ACL entries
	var acl schema.ACLList
	for _, aclStr := range cmd.Acl {
		item, err := schema.ParseACLItem(aclStr)
		if err != nil {
			return fmt.Errorf("invalid ACL %q: %w", aclStr, err)
		}
		acl = append(acl, item)
	}

	// Create database
	database, err := client.CreateDatabase(ctx.ctx, schema.DatabaseMeta{
		Name:  cmd.Name,
		Owner: cmd.Owner,
		Acl:   acl,
	})
	if err != nil {
		return err
	}

	// Print
	fmt.Println(database)
	return nil
}

func (cmd *DeleteDatabaseCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Delete database
	if err := client.DeleteDatabase(ctx.ctx, cmd.Name); err != nil {
		return err
	}

	// Return success
	return nil
}

func (cmd *UpdateDatabaseCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Parse ACL entries
	var acl schema.ACLList
	for _, aclStr := range cmd.Acl {
		item, err := schema.ParseACLItem(aclStr)
		if err != nil {
			return fmt.Errorf("invalid ACL %q: %w", aclStr, err)
		}
		acl = append(acl, item)
	}

	// Build meta
	meta := schema.DatabaseMeta{
		Owner: cmd.Owner,
		Acl:   acl,
	}
	if cmd.NewName != "" {
		meta.Name = cmd.NewName
	}

	// Update database
	database, err := client.UpdateDatabase(ctx.ctx, cmd.Name, meta)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(database)
	return nil
}
