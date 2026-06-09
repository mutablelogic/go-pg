package main

import (
	"fmt"

	// Packages
	httpclient "github.com/mutablelogic/go-pg/pkg/manager/httpclient"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type StatementCommands struct {
	ListStatement  ListStatementCommand  `cmd:"" name:"statements" help:"List query statistics from pg_stat_statements."`
	ResetStatement ResetStatementCommand `cmd:"" name:"reset-statements" help:"Reset all statement statistics."`
}

type ListStatementCommand struct {
	Database string  `name:"database" help:"Filter by database name"`
	Role     string  `name:"role" help:"Filter by role name"`
	Sort     string  `name:"sort" help:"Sort by field (calls, rows, total_ms, min_ms, max_ms, mean_ms)"`
	Offset   uint64  `name:"offset" help:"Offset for pagination"`
	Limit    *uint64 `name:"limit" help:"Limit for pagination"`
}

type ResetStatementCommand struct{}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *ListStatementCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Build options
	opts := []httpclient.Opt{httpclient.WithOffsetLimit(cmd.Offset, cmd.Limit)}
	if cmd.Database != "" {
		opts = append(opts, httpclient.WithDatabase(&cmd.Database))
	}
	if cmd.Role != "" {
		opts = append(opts, httpclient.WithRole(&cmd.Role))
	}
	if cmd.Sort != "" {
		opts = append(opts, httpclient.WithSort(cmd.Sort))
	}

	// List statements
	statements, err := client.ListStatements(ctx.ctx, opts...)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(statements)
	return nil
}

func (cmd *ResetStatementCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Reset statements
	if err := client.ResetStatements(ctx.ctx); err != nil {
		return err
	}

	fmt.Println("Statement statistics reset successfully")
	return nil
}
