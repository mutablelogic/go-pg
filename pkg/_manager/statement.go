package manager

import (
	"context"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListStatements returns a list of statement statistics from pg_stat_statements.
// The request can filter by database, user, and order by various fields.
// Returns ErrNotAvailable if pg_stat_statements is not installed.
func (manager *Manager) ListStatements(ctx context.Context, req schema.StatementListRequest) (*schema.StatementList, error) {
	if !manager.statStatementsAvailable {
		return nil, pg.ErrNotAvailable.With("pg_stat_statements")
	}

	// Execute query
	var list schema.StatementList
	if err := manager.conn.List(ctx, &list, &req); err != nil {
		return nil, err
	}

	return &list, nil
}

// ResetStatements resets the statistics for all statements.
// Returns ErrNotAvailable if pg_stat_statements is not installed.
func (manager *Manager) ResetStatements(ctx context.Context) error {
	if !manager.statStatementsAvailable {
		return pg.ErrNotAvailable.With("pg_stat_statements")
	}

	// Execute reset
	return manager.conn.Exec(ctx, statementReset)
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE CONSTANTS

const statementReset = `SELECT public.pg_stat_statements_reset()`
