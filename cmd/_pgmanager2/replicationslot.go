package main

import (
	"fmt"

	// Packages
	httpclient "github.com/mutablelogic/go-pg/pkg/manager/httpclient"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ReplicationSlotCommands struct {
	ListReplicationSlot   ListReplicationSlotCommand   `cmd:"" name:"replication-slots" help:"List replication slots."`
	GetReplicationSlot    GetReplicationSlotCommand    `cmd:"" name:"replication-slot" help:"Get replication slot."`
	CreateReplicationSlot CreateReplicationSlotCommand `cmd:"" name:"create-replication-slot" help:"Create replication slot."`
	DeleteReplicationSlot DeleteReplicationSlotCommand `cmd:"" name:"delete-replication-slot" help:"Delete replication slot."`
}

type ListReplicationSlotCommand struct {
	Offset uint64  `name:"offset" help:"Offset for pagination"`
	Limit  *uint64 `name:"limit" help:"Limit for pagination"`
}

type GetReplicationSlotCommand struct {
	Name string `arg:"" name:"name" help:"Replication slot name"`
}

type DeleteReplicationSlotCommand struct {
	GetReplicationSlotCommand
}

type CreateReplicationSlotCommand struct {
	Name      string `arg:"" name:"name" help:"Replication slot name"`
	Type      string `name:"type" required:"" enum:"physical,logical" help:"Slot type (physical or logical)"`
	Plugin    string `name:"plugin" help:"Output plugin for logical slots (e.g., pgoutput)"`
	Database  string `name:"database" help:"Database for logical slots"`
	Temporary bool   `name:"temporary" help:"Create a temporary slot"`
	TwoPhase  bool   `name:"two-phase" help:"Enable two-phase commit support (PG14+)"`
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *ListReplicationSlotCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// List replication slots
	slots, err := client.ListReplicationSlots(ctx.ctx, httpclient.WithOffsetLimit(cmd.Offset, cmd.Limit))
	if err != nil {
		return err
	}

	// Print
	fmt.Println(slots)
	return nil
}

func (cmd *GetReplicationSlotCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Get one replication slot
	slot, err := client.GetReplicationSlot(ctx.ctx, cmd.Name)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(slot)
	return nil
}

func (cmd *CreateReplicationSlotCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Create replication slot
	slot, err := client.CreateReplicationSlot(ctx.ctx, schema.ReplicationSlotMeta{
		Name:      cmd.Name,
		Type:      cmd.Type,
		Plugin:    cmd.Plugin,
		Database:  cmd.Database,
		Temporary: cmd.Temporary,
		TwoPhase:  cmd.TwoPhase,
	})
	if err != nil {
		return err
	}

	// Print
	fmt.Println(slot)
	return nil
}

func (cmd *DeleteReplicationSlotCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Delete replication slot
	if err := client.DeleteReplicationSlot(ctx.ctx, cmd.Name); err != nil {
		return err
	}

	// Return success
	return nil
}
