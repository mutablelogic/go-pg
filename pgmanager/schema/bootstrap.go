package schema

import (
	"context"
	"fmt"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// BOOTSTRAP

// BootstrapResult contains the result of the bootstrap process
type BootstrapResult struct {
	// StatStatementsAvailable indicates if pg_stat_statements extension is available
	StatStatementsAvailable bool

	// Replica is true when the server is a replica, in recovery, which is
	// read only, so nothing is created, as it's the primary's
	Replica bool
}

// recovery is whether the server is in recovery, as a replica is, and
// extension whether an extension is installed
type recovery bool
type extension bool

func (r *recovery) Scan(row pg.Row) error  { return row.Scan((*bool)(r)) }
func (e *extension) Scan(row pg.Row) error { return row.Scan((*bool)(e)) }

// query is a query, as a selector
type query string

func (q query) Select(*pg.Bind, pg.Op) (string, error) { return string(q), nil }

// Bootstrap creates required extensions for the manager.
// - dblink: Required for remote database queries
// - pg_stat_statements: Optional, for query statistics (requires shared_preload_libraries)
// This should be called once when initializing the manager.
func Bootstrap(ctx context.Context, conn pg.PoolConn) (*BootstrapResult, error) {
	result := &BootstrapResult{}

	// A replica is read only, so its extensions, and its roles'
	// descriptions, are the primary's, which are checked, not created
	var inRecovery recovery
	if err := conn.Get(ctx, &inRecovery, query(inRecoveryQuery)); err != nil {
		return nil, err
	}
	result.Replica = bool(inRecovery)
	if result.Replica {
		var dblink extension
		if err := conn.Get(ctx, &dblink, query(dblinkExistsQuery)); err != nil {
			return nil, err
		} else if !dblink {
			return nil, pg.ErrNotAvailable.With("the server is a replica, without the dblink extension, which pgmanager needs, so it's created on the primary, such as by running pgmanager there")
		}
		if err := conn.Exec(ctx, statStatementsVerify); err == nil {
			result.StatStatementsAvailable = true
		}
		return result, nil
	}

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

	// Describe PostgreSQL's predefined roles, which have no descriptions,
	// so what they are is seen. A role which has one keeps it, one which
	// this version of PostgreSQL hasn't is skipped, and without the right to
	// comment on them, as only a superuser has, they're left (optional)
	for _, role := range predefinedRoles {
		_ = conn.Exec(ctx, fmt.Sprintf(predefinedRoleComment, types.Quote(role.name), types.DoubleQuote(role.name), types.Quote(role.description)))
	}

	return result, nil
}

// predefinedRoles are PostgreSQL's predefined roles, and what each is for
var predefinedRoles = []struct{ name, description string }{
	{"pg_read_all_data", "Reads every table, view and sequence, and uses every schema, as if it had SELECT and USAGE on them"},
	{"pg_write_all_data", "Writes to every table, view and sequence, and uses every schema, as if it had INSERT, UPDATE, DELETE and USAGE on them"},
	{"pg_read_all_settings", "Reads every setting, including those only a superuser can"},
	{"pg_read_all_stats", "Reads every pg_stat_* view, and uses the statistics extensions, including what only a superuser can"},
	{"pg_stat_scan_tables", "Runs monitoring functions which can lock tables, for a while"},
	{"pg_monitor", "Monitors the server, as a member of pg_read_all_settings, pg_read_all_stats and pg_stat_scan_tables"},
	{"pg_database_owner", "The owner of the current database, who's its only member"},
	{"pg_signal_backend", "Cancels another session's query, or ends the session"},
	{"pg_read_server_files", "Reads files on the server, wherever the database can, with COPY and file functions"},
	{"pg_write_server_files", "Writes files on the server, wherever the database can, with COPY and file functions"},
	{"pg_execute_server_program", "Runs programs on the server, as the user the database runs as, with COPY and other functions"},
	{"pg_checkpoint", "Runs CHECKPOINT"},
	{"pg_maintain", "Runs VACUUM, ANALYZE, CLUSTER, REFRESH MATERIALIZED VIEW, REINDEX and LOCK TABLE on every table, as if it had MAINTAIN on them"},
	{"pg_use_reserved_connections", "Uses the connections reserved by reserved_connections"},
	{"pg_create_subscription", "Creates subscriptions, with CREATE on the database"},
	{"pg_signal_autovacuum_worker", "Cancels an autovacuum worker's work, or ends it"},
}

const (
	// predefinedRoleComment describes a role, by its name, a literal, and
	// then an identifier, when it exists, and has no description. Its body
	// is quoted with $$, as a query's other $names are its variables.
	predefinedRoleComment = `DO $$ BEGIN
		IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = %s AND shobj_description(oid, 'pg_authid') IS NULL) THEN
			COMMENT ON ROLE %s IS %s;
		END IF;
	END $$`

	dblinkCreateExtension         = `CREATE EXTENSION IF NOT EXISTS dblink WITH SCHEMA ` + defaultSchema
	statStatementsCreateExtension = `CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA ` + defaultSchema
	statStatementsVerify          = `SELECT 1 FROM public.pg_stat_statements LIMIT 1`
	inRecoveryQuery               = `SELECT pg_is_in_recovery()`
	dblinkExistsQuery             = `SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'dblink')`
)
