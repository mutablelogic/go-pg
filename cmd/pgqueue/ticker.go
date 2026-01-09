package main

import (
	"encoding/json"
	"fmt"
	"time"

	// Packages

	client "github.com/mutablelogic/go-client"
	httpclient "github.com/mutablelogic/go-pg/pkg/queue/httpclient"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type TickerCommands struct {
	ListTicker   ListTickerCommand   `cmd:"" name:"tickers" help:"List tickers." group:"TICKER"`
	GetTicker    GetTickerCommand    `cmd:"" name:"ticker" help:"Get ticker." group:"TICKER"`
	CreateTicker CreateTickerCommand `cmd:"" name:"create-ticker" help:"Create ticker." group:"TICKER"`
	DeleteTicker DeleteTickerCommand `cmd:"" name:"delete-ticker" help:"Delete ticker." group:"TICKER"`
	UpdateTicker UpdateTickerCommand `cmd:"" name:"update-ticker" help:"Update ticker." group:"TICKER"`
	NextTicker   NextTickerCommand   `cmd:"" name:"next-ticker" help:"Stream matured tickers (SSE)." group:"TICKER"`
}

type ListTickerCommand struct {
	Offset uint64  `name:"offset" help:"Offset for pagination"`
	Limit  *uint64 `name:"limit" help:"Limit for pagination"`
}

type GetTickerCommand struct {
	Name string `arg:"" name:"name" help:"Ticker name"`
}

type CreateTickerCommand struct {
	Name     string         `arg:"" name:"name" help:"Ticker name"`
	Interval *time.Duration `name:"interval" help:"Ticker interval (default 1 minute)"`
	Payload  string         `name:"payload" help:"Ticker payload (JSON)"`
}

type DeleteTickerCommand struct {
	Name string `arg:"" name:"name" help:"Ticker name"`
}

type UpdateTickerCommand struct {
	Name     string         `arg:"" name:"name" help:"Ticker name"`
	Interval *time.Duration `name:"interval" help:"Ticker interval"`
	Payload  string         `name:"payload" help:"Ticker payload (JSON)"`
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *ListTickerCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// List tickers
	tickers, err := client.ListTickers(ctx.ctx, httpclient.WithOffsetLimit(cmd.Offset, cmd.Limit))
	if err != nil {
		return err
	}

	// Print
	fmt.Println(tickers)
	return nil
}

func (cmd *GetTickerCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Get one ticker
	ticker, err := client.GetTicker(ctx.ctx, cmd.Name)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(ticker)
	return nil
}

func (cmd *CreateTickerCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Parse payload
	var payload json.RawMessage
	if cmd.Payload != "" {
		if !json.Valid([]byte(cmd.Payload)) {
			return fmt.Errorf("invalid payload JSON")
		}
		payload = json.RawMessage(cmd.Payload)
	}

	// Create ticker
	ticker, err := client.CreateTicker(ctx.ctx, schema.TickerMeta{
		Ticker:   cmd.Name,
		Interval: cmd.Interval,
		Payload:  payload,
	})
	if err != nil {
		return err
	}

	// Print
	fmt.Println(ticker)
	return nil
}

func (cmd *DeleteTickerCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Delete ticker
	ticker, err := client.DeleteTicker(ctx.ctx, cmd.Name)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(ticker)
	return nil
}

func (cmd *UpdateTickerCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Parse payload
	var payload json.RawMessage
	if cmd.Payload != "" {
		if !json.Valid([]byte(cmd.Payload)) {
			return fmt.Errorf("invalid payload JSON")
		}
		payload = json.RawMessage(cmd.Payload)
	}

	// Update ticker
	ticker, err := client.UpdateTicker(ctx.ctx, cmd.Name, schema.TickerMeta{
		Interval: cmd.Interval,
		Payload:  payload,
	})
	if err != nil {
		return err
	}

	// Print
	fmt.Println(ticker)
	return nil
}

type NextTickerCommand struct{}

func (cmd *NextTickerCommand) Run(ctx *Globals) error {
	c, err := ctx.Client()
	if err != nil {
		return err
	}

	// Stream matured tickers via SSE, reconnecting on EOF/timeout
	for {
		err := c.NextTicker(ctx.ctx, func(event client.TextStreamEvent) error {
			fmt.Printf("[%s] %s\n", event.Event, event.Data)
			return nil
		})

		// Check if context was cancelled (Ctrl+C)
		if ctx.ctx.Err() != nil {
			return nil
		}

		// Log reconnection and retry
		if err != nil {
			fmt.Printf("[reconnecting] %v\n", err)
		}
	}
}
