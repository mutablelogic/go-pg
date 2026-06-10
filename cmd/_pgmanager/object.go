package main

import (
	"fmt"

	// Packages
	httpclient "github.com/mutablelogic/go-pg/pkg/manager/httpclient"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ObjectCommands struct {
	ListObjects ListObjectsCommand `cmd:"" name:"objects" help:"List objects."`
	GetObject   GetObjectCommand   `cmd:"" name:"object" help:"Get object."`
}

type ListObjectsCommand struct {
	Database  string  `name:"database" short:"d" help:"Filter by database name"`
	Namespace string  `name:"schema" short:"s" help:"Filter by schema (namespace) name"`
	Type      string  `name:"type" short:"t" help:"Filter by object type (TABLE, VIEW, INDEX, SEQUENCE, etc.)"`
	Offset    uint64  `name:"offset" help:"Offset for pagination"`
	Limit     *uint64 `name:"limit" help:"Limit for pagination"`
}

type GetObjectCommand struct {
	Database  string `arg:"" name:"database" help:"Database name"`
	Namespace string `arg:"" name:"schema" help:"Schema (namespace) name"`
	Name      string `arg:"" name:"name" help:"Object name"`
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *ListObjectsCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// List objects
	objects, err := client.ListObjects(ctx.ctx, cmd.Database, cmd.Namespace, httpclient.WithOffsetLimit(cmd.Offset, cmd.Limit))
	if err != nil {
		return err
	}

	// Filter by type if specified (client-side filtering since API may not support it in path)
	if cmd.Type != "" {
		var filtered []interface{}
		for _, obj := range objects.Body {
			if obj.Type == cmd.Type {
				filtered = append(filtered, obj)
			}
		}
		fmt.Printf("Count: %d (filtered from %d)\n", len(filtered), objects.Count)
		for _, obj := range filtered {
			fmt.Println(obj)
		}
		return nil
	}

	// Print
	fmt.Println(objects)
	return nil
}

func (cmd *GetObjectCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Get one object
	obj, err := client.GetObject(ctx.ctx, cmd.Database, cmd.Namespace, cmd.Name)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(obj)
	return nil
}
