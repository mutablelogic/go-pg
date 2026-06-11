package manager

import (
	"context"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListStatements returns a list of statement statistics from pg_stat_statements.
// The request can filter by database, user, and order by various fields.
// Returns ErrNotAvailable if pg_stat_statements is not installed.
func (manager *Manager) ListStatements(ctx context.Context, req schema.StatementListRequest) (*schema.StatementList, error) {
	if !manager.pgStatementsAvailable {
		return nil, pg.ErrNotAvailable.With("pg_stat_statements")
	}

	// Execute query
	var result schema.StatementList
	if err := manager.conn.List(ctx, &result, &req); err != nil {
		return nil, err
	}

	// Set the offset and limit in the result to reflect the actual count of items returned
	// which may be less than the requested limit if there are not enough items in the database.
	result.StatementListRequest = req
	result.OffsetLimit.Clamp(result.Count)

	return &result, nil
}

// ResetStatements resets the statistics for all statements.
// Returns ErrNotAvailable if pg_stat_statements is not installed.
func (manager *Manager) ResetStatements(ctx context.Context) error {
	if !manager.pgStatementsAvailable {
		return pg.ErrNotAvailable.With("pg_stat_statements")
	}

	// Execute reset
	return manager.conn.Exec(ctx, statementReset)
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE CONSTANTS

const statementReset = `SELECT public.pg_stat_statements_reset()`
