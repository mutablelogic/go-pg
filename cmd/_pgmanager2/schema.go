package main

import (
	"fmt"

	// Packages
	httpclient "github.com/mutablelogic/go-pg/pkg/manager/httpclient"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type SchemaCommands struct {
	ListSchema   ListSchemaCommand   `cmd:"" name:"schemas" help:"List schemas."`
	GetSchema    GetSchemaCommand    `cmd:"" name:"schema" help:"Get schema."`
	CreateSchema CreateSchemaCommand `cmd:"" name:"create-schema" help:"Create schema."`
	DeleteSchema DeleteSchemaCommand `cmd:"" name:"delete-schema" help:"Delete schema."`
	UpdateSchema UpdateSchemaCommand `cmd:"" name:"update-schema" help:"Update schema."`
}

type ListSchemaCommand struct {
	Database string  `name:"database" short:"d" help:"Filter by database name"`
	Offset   uint64  `name:"offset" help:"Offset for pagination"`
	Limit    *uint64 `name:"limit" help:"Limit for pagination"`
}

type GetSchemaCommand struct {
	Database  string `arg:"" name:"database" help:"Database name"`
	Namespace string `arg:"" name:"namespace" help:"Schema (namespace) name"`
}

type DeleteSchemaCommand struct {
	GetSchemaCommand
	Force bool `name:"force" help:"Force delete with CASCADE"`
}

type CreateSchemaCommand struct {
	Database string   `arg:"" name:"database" help:"Database name"`
	Name     string   `arg:"" name:"name" help:"Schema name"`
	Owner    string   `name:"owner" help:"Schema owner (defaults to current user)"`
	Acl      []string `name:"acl" help:"Access control list entries (format: role:priv,priv,... e.g. myuser:USAGE,CREATE)"`
}

type UpdateSchemaCommand struct {
	GetSchemaCommand
	NewName string   `name:"rename" help:"Rename schema to this name"`
	Owner   string   `name:"owner" help:"Schema owner"`
	Acl     []string `name:"acl" help:"Access control list entries (format: role:priv,priv,... e.g. myuser:USAGE,CREATE)"`
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *ListSchemaCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// List schemas
	schemas, err := client.ListSchemas(ctx.ctx, cmd.Database, httpclient.WithOffsetLimit(cmd.Offset, cmd.Limit))
	if err != nil {
		return err
	}

	// Print
	fmt.Println(schemas)
	return nil
}

func (cmd *GetSchemaCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Get one schema
	s, err := client.GetSchema(ctx.ctx, cmd.Database, cmd.Namespace)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(s)
	return nil
}

func (cmd *CreateSchemaCommand) Run(ctx *Globals) error {
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

	// Create schema
	s, err := client.CreateSchema(ctx.ctx, cmd.Database, schema.SchemaMeta{
		Name:  cmd.Name,
		Owner: cmd.Owner,
		Acl:   acl,
	})
	if err != nil {
		return err
	}

	// Print
	fmt.Println(s)
	return nil
}

func (cmd *DeleteSchemaCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Delete schema
	if err := client.DeleteSchema(ctx.ctx, cmd.Database, cmd.Namespace, httpclient.WithForce(cmd.Force)); err != nil {
		return err
	}

	// Return success
	return nil
}

func (cmd *UpdateSchemaCommand) Run(ctx *Globals) error {
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
	meta := schema.SchemaMeta{
		Owner: cmd.Owner,
		Acl:   acl,
	}
	if cmd.NewName != "" {
		meta.Name = cmd.NewName
	}

	// Update schema
	s, err := client.UpdateSchema(ctx.ctx, cmd.Database, cmd.Namespace, meta)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(s)
	return nil
}
