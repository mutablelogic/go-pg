package main

import (
	"fmt"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type NamespaceCommands struct {
	ListNamespace ListNamespaceCommand `cmd:"" name:"namespaces" help:"List namespaces." group:"QUEUE"`
}

type ListNamespaceCommand struct{}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *ListNamespaceCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// List namespaces
	namespaces, err := client.ListNamespaces(ctx.ctx)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(namespaces)
	return nil
}
