package schema

import (
	"context"

	// Packages
	pg "github.com/mutablelogic/go-pg"
)

////////////////////////////////////////////////////////////////////////////////
// BOOTSTRAP

// BootstrapResult contains the result of the bootstrap process
type BootstrapResult struct {
	// StatStatementsAvailable indicates if pg_stat_statements extension is available
	StatStatementsAvailable bool
}

// Bootstrap creates required extensions for the manager.
// - dblink: Required for remote database queries
// - pg_stat_statements: Optional, for query statistics (requires shared_preload_libraries)
// This should be called once when initializing the manager.
func Bootstrap(ctx context.Context, conn pg.PoolConn) (*BootstrapResult, error) {
	result := &BootstrapResult{}

	// Create dblink extension (required)
	if err := conn.Exec(ctx, dblinkCreateExtension); err != nil {
		return nil, err
	}

	// Try to create and verify pg_stat_statements extension (optional)
	// Creating the extension can succeed but querying fails if not in shared_preload_libraries
	if err := conn.Exec(ctx, statStatementsCreateExtension); err == nil {
		// Verify we can actually query the view (fails if not in shared_preload_libraries)
		if err := conn.Exec(ctx, statStatementsVerify); err == nil {
			result.StatStatementsAvailable = true
		}
	}

	return result, nil
}

const (
	dblinkCreateExtension         = `CREATE EXTENSION IF NOT EXISTS dblink WITH SCHEMA ` + defaultSchema
	statStatementsCreateExtension = `CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA ` + defaultSchema
	statStatementsVerify          = `SELECT 1 FROM public.pg_stat_statements LIMIT 1`
)
