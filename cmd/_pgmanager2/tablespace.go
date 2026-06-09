package main

import (
	"fmt"

	// Packages
	httpclient "github.com/mutablelogic/go-pg/pkg/manager/httpclient"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type TablespaceCommands struct {
	ListTablespace   ListTablespaceCommand   `cmd:"" name:"tablespaces" help:"List tablespaces."`
	GetTablespace    GetTablespaceCommand    `cmd:"" name:"tablespace" help:"Get tablespace."`
	CreateTablespace CreateTablespaceCommand `cmd:"" name:"create-tablespace" help:"Create tablespace."`
	DeleteTablespace DeleteTablespaceCommand `cmd:"" name:"delete-tablespace" help:"Delete tablespace."`
	UpdateTablespace UpdateTablespaceCommand `cmd:"" name:"update-tablespace" help:"Update tablespace."`
}

type ListTablespaceCommand struct {
	Offset uint64  `name:"offset" help:"Offset for pagination"`
	Limit  *uint64 `name:"limit" help:"Limit for pagination"`
}

type GetTablespaceCommand struct {
	Name string `arg:"" name:"name" help:"Tablespace name"`
}

type DeleteTablespaceCommand struct {
	GetTablespaceCommand
}

type CreateTablespaceCommand struct {
	GetTablespaceCommand
	Location string   `name:"location" required:"" help:"Absolute path to tablespace directory"`
	Owner    string   `name:"owner" help:"Tablespace owner"`
	Acl      []string `name:"acl" help:"Access control list entries (format: role:priv,priv,... e.g. myuser:CREATE)"`
}

type UpdateTablespaceCommand struct {
	GetTablespaceCommand
	NewName string   `name:"rename" help:"Rename tablespace to this name"`
	Owner   string   `name:"owner" help:"Tablespace owner"`
	Acl     []string `name:"acl" help:"Access control list entries (format: role:priv,priv,... e.g. myuser:CREATE)"`
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *ListTablespaceCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// List tablespaces
	tablespaces, err := client.ListTablespaces(ctx.ctx, httpclient.WithOffsetLimit(cmd.Offset, cmd.Limit))
	if err != nil {
		return err
	}

	// Print
	fmt.Println(tablespaces)
	return nil
}

func (cmd *GetTablespaceCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Get one tablespace
	tablespace, err := client.GetTablespace(ctx.ctx, cmd.Name)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(tablespace)
	return nil
}

func (cmd *CreateTablespaceCommand) Run(ctx *Globals) error {
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

	// Create tablespace
	tablespace, err := client.CreateTablespace(ctx.ctx, schema.TablespaceMeta{
		Name:  cmd.Name,
		Owner: cmd.Owner,
		Acl:   acl,
	}, cmd.Location)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(tablespace)
	return nil
}

func (cmd *DeleteTablespaceCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Delete tablespace
	if err := client.DeleteTablespace(ctx.ctx, cmd.Name); err != nil {
		return err
	}

	// Return success
	return nil
}

func (cmd *UpdateTablespaceCommand) Run(ctx *Globals) error {
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
	meta := schema.TablespaceMeta{
		Owner: cmd.Owner,
		Acl:   acl,
	}
	if cmd.NewName != "" {
		meta.Name = cmd.NewName
	}

	// Update tablespace
	tablespace, err := client.UpdateTablespace(ctx.ctx, cmd.Name, meta)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(tablespace)
	return nil
}
