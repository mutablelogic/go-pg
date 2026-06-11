package main

import (
	"fmt"
	"time"

	// Packages
	httpclient "github.com/mutablelogic/go-pg/pkg/queue/httpclient"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type QueueCommands struct {
	ListQueue   ListQueueCommand   `cmd:"" name:"queues" help:"List queues." group:"QUEUE"`
	GetQueue    GetQueueCommand    `cmd:"" name:"queue" help:"Get queue." group:"QUEUE"`
	CreateQueue CreateQueueCommand `cmd:"" name:"create-queue" help:"Create queue." group:"QUEUE"`
	DeleteQueue DeleteQueueCommand `cmd:"" name:"delete-queue" help:"Delete queue." group:"QUEUE"`
	UpdateQueue UpdateQueueCommand `cmd:"" name:"update-queue" help:"Update queue." group:"QUEUE"`
}

type ListQueueCommand struct {
	Offset uint64  `name:"offset" help:"Offset for pagination"`
	Limit  *uint64 `name:"limit" help:"Limit for pagination"`
}

type GetQueueCommand struct {
	Name string `arg:"" name:"name" help:"Queue name"`
}

type CreateQueueCommand struct {
	Name       string         `arg:"" name:"name" help:"Queue name"`
	TTL        *time.Duration `name:"ttl" help:"Time-to-live for queue messages"`
	Retries    *uint64        `name:"retries" help:"Number of retries before failing"`
	RetryDelay *time.Duration `name:"retry-delay" help:"Backoff delay between retries"`
}

type DeleteQueueCommand struct {
	Name string `arg:"" name:"name" help:"Queue name"`
}

type UpdateQueueCommand struct {
	Name       string         `arg:"" name:"name" help:"Queue name"`
	TTL        *time.Duration `name:"ttl" help:"Time-to-live for queue messages"`
	Retries    *uint64        `name:"retries" help:"Number of retries before failing"`
	RetryDelay *time.Duration `name:"retry-delay" help:"Backoff delay between retries"`
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *ListQueueCommand) Run(ctx *Globals) (err error) {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// OTEL
	parent, endSpan := ctx.StartSpan("ListQueueCommand")
	defer func() { endSpan(err) }()

	// List queues
	queues, err := client.ListQueues(parent, httpclient.WithOffsetLimit(cmd.Offset, cmd.Limit))
	if err != nil {
		return err
	}

	// Print
	fmt.Println(queues)
	return nil
}

func (cmd *GetQueueCommand) Run(ctx *Globals) (err error) {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// OTEL
	parent, endSpan := ctx.StartSpan("GetQueueCommand")
	defer func() { endSpan(err) }()

	// Get one queue
	queue, err := client.GetQueue(parent, cmd.Name)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(queue)
	return nil
}

func (cmd *CreateQueueCommand) Run(ctx *Globals) (err error) {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// OTEL
	parent, endSpan := ctx.StartSpan("CreateQueueCommand")
	defer func() { endSpan(err) }()

	// Create queue
	queue, err := client.CreateQueue(parent, schema.QueueMeta{
		Queue:      cmd.Name,
		TTL:        cmd.TTL,
		Retries:    cmd.Retries,
		RetryDelay: cmd.RetryDelay,
	})
	if err != nil {
		return err
	}

	// Print
	fmt.Println(queue)
	return nil
}

func (cmd *DeleteQueueCommand) Run(ctx *Globals) (err error) {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// OTEL
	parent, endSpan := ctx.StartSpan("DeleteQueueCommand")
	defer func() { endSpan(err) }()

	// Delete queue
	queue, err := client.DeleteQueue(parent, cmd.Name)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(queue)
	return nil
}

func (cmd *UpdateQueueCommand) Run(ctx *Globals) (err error) {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// OTEL
	parent, endSpan := ctx.StartSpan("UpdateQueueCommand")
	defer func() { endSpan(err) }()

	// Update queue
	queue, err := client.UpdateQueue(parent, cmd.Name, schema.QueueMeta{
		TTL:        cmd.TTL,
		Retries:    cmd.Retries,
		RetryDelay: cmd.RetryDelay,
	})
	if err != nil {
		return err
	}

	// Print
	fmt.Println(queue)
	return nil
}
