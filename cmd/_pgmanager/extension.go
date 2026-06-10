package main

import (
	"fmt"

	// Packages
	httpclient "github.com/mutablelogic/go-pg/pkg/manager/httpclient"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ExtensionCommands struct {
	ListExtension   ListExtensionCommand   `cmd:"" name:"extensions" help:"List extensions."`
	GetExtension    GetExtensionCommand    `cmd:"" name:"extension" help:"Get extension."`
	CreateExtension CreateExtensionCommand `cmd:"" name:"create-extension" help:"Create extension."`
	DeleteExtension DeleteExtensionCommand `cmd:"" name:"delete-extension" help:"Delete extension."`
	UpdateExtension UpdateExtensionCommand `cmd:"" name:"update-extension" help:"Update extension."`
}

type ListExtensionCommand struct {
	Database  string  `name:"database" help:"Filter by database name"`
	Installed *bool   `name:"installed" help:"Filter by installed status (true/false)"`
	Offset    uint64  `name:"offset" help:"Offset for pagination"`
	Limit     *uint64 `name:"limit" help:"Limit for pagination"`
}

type GetExtensionCommand struct {
	Name string `arg:"" name:"name" help:"Extension name"`
}

type DeleteExtensionCommand struct {
	GetExtensionCommand
	Database string `arg:"" required:"" name:"database" help:"Database to delete extension from"`
	Cascade  bool   `name:"cascade" help:"Cascade to dependent objects"`
}

type CreateExtensionCommand struct {
	GetExtensionCommand
	Database string `arg:"" required:"" name:"database" help:"Database to install extension into"`
	Schema   string `name:"schema" help:"Schema to install extension into"`
	Version  string `name:"version" help:"Extension version"`
	Cascade  bool   `name:"cascade" help:"Cascade to dependent objects"`
}

type UpdateExtensionCommand struct {
	GetExtensionCommand
	Database string `arg:"" required:"" name:"database" help:"Database containing the extension"`
	Version  string `name:"version" help:"Update to this version"`
	Schema   string `name:"schema" help:"Move extension to this schema (only for relocatable extensions)"`
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *ListExtensionCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Build options
	opts := []httpclient.Opt{httpclient.WithOffsetLimit(cmd.Offset, cmd.Limit)}
	if cmd.Database != "" {
		opts = append(opts, httpclient.OptDatabase(cmd.Database))
		// When a database is specified, default to showing only installed extensions
		if cmd.Installed == nil {
			installed := true
			opts = append(opts, httpclient.WithInstalled(&installed))
		} else {
			opts = append(opts, httpclient.WithInstalled(cmd.Installed))
		}
	} else if cmd.Installed != nil {
		opts = append(opts, httpclient.WithInstalled(cmd.Installed))
	}

	// List extensions
	extensions, err := client.ListExtensions(ctx.ctx, opts...)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(extensions)
	return nil
}

func (cmd *GetExtensionCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Get one extension
	extension, err := client.GetExtension(ctx.ctx, cmd.Name)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(extension)
	return nil
}

func (cmd *CreateExtensionCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Build options
	opts := []httpclient.Opt{}
	if cmd.Cascade {
		opts = append(opts, httpclient.OptCascade(true))
	}

	// Create extension
	extension, err := client.CreateExtension(ctx.ctx, schema.ExtensionMeta{
		Name:     cmd.Name,
		Database: cmd.Database,
		Schema:   cmd.Schema,
		Version:  cmd.Version,
	}, opts...)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(extension)
	return nil
}

func (cmd *DeleteExtensionCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Build options
	opts := []httpclient.Opt{
		httpclient.OptDatabase(cmd.Database),
	}
	if cmd.Cascade {
		opts = append(opts, httpclient.OptCascade(true))
	}

	// Delete extension
	if err := client.DeleteExtension(ctx.ctx, cmd.Name, opts...); err != nil {
		return err
	}

	// Return success
	return nil
}

func (cmd *UpdateExtensionCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Update extension
	extension, err := client.UpdateExtension(ctx.ctx, cmd.Name, schema.ExtensionMeta{
		Database: cmd.Database,
		Version:  cmd.Version,
		Schema:   cmd.Schema,
	})
	if err != nil {
		return err
	}

	// Print
	fmt.Println(extension)
	return nil
}
