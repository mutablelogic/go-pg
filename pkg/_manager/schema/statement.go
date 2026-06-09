package schema

import (
	"encoding/json"
	"strings"

	// Packages
	pg "github.com/mutablelogic/go-pg"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Statement represents a row from pg_stat_statements
type Statement struct {
	Role     string  `json:"role,omitempty"`     // Name of the role who executed the statement
	Database string  `json:"database,omitempty"` // Name of the database in which the statement was executed
	QueryID  int64   `json:"query_id"`           // Hash code to identify identical normalized queries
	Query    string  `json:"query"`              // Text of a representative statement
	Calls    int64   `json:"calls"`              // Number of times the statement was executed
	Rows     int64   `json:"rows"`               // Total number of rows retrieved or affected by the statement
	Total    float64 `json:"total_ms"`           // Total time spent executing the statement, in milliseconds
	Min      float64 `json:"min_ms"`             // Minimum time spent executing the statement, in milliseconds
	Max      float64 `json:"max_ms"`             // Maximum time spent executing the statement, in milliseconds
	Mean     float64 `json:"mean_ms"`            // Mean time spent executing the statement, in milliseconds
}

// StatementList is a list of statements with a total count
type StatementList struct {
	Count uint64      `json:"count"`
	Body  []Statement `json:"body"`
}

// StatementListRequest contains parameters for listing statements
type StatementListRequest struct {
	pg.OffsetLimit

	// Filter by database name
	Database *string `json:"database,omitempty"`

	// Filter by role name
	Role *string `json:"role,omitempty"`

	// Sort by field (calls, rows, total_ms, min_ms, max_ms, mean_ms)
	// All sort DESC except min_ms which sorts ASC
	Sort string `json:"sort,omitempty"`
}

///////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (s Statement) String() string {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (l StatementList) String() string {
	data, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

///////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (r StatementListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Build WHERE clause
	var where []string
	if r.Database != nil && *r.Database != "" {
		bind.Set("database", *r.Database)
		where = append(where, "d.datname = @database")
	}
	if r.Role != nil && *r.Role != "" {
		bind.Set("role", *r.Role)
		where = append(where, "u.rolname = @role")
	}

	if len(where) > 0 {
		bind.Set("where", "WHERE "+strings.Join(where, " AND "))
	} else {
		bind.Set("where", "")
	}

	// Build ORDER BY clause - always order by database, query_id first
	var sortClause string
	switch strings.ToLower(r.Sort) {
	case "":
		// No additional sort
	case "calls":
		sortClause = ", calls DESC"
	case "rows":
		sortClause = ", rows DESC"
	case "total_ms":
		sortClause = ", total_exec_time DESC"
	case "min_ms":
		sortClause = ", min_exec_time ASC"
	case "max_ms":
		sortClause = ", max_exec_time DESC"
	case "mean_ms":
		sortClause = ", mean_exec_time DESC"
	default:
		return "", pg.ErrBadParameter.Withf("invalid sort parameter %q", r.Sort)
	}
	bind.Set("orderby", "ORDER BY database ASC, queryid ASC"+sortClause)

	// Set offset/limit
	r.OffsetLimit.Bind(bind, StatementListLimit)

	// Return query
	switch op {
	case pg.List:
		return statementList, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported StatementListRequest operation %q", op)
	}
}

///////////////////////////////////////////////////////////////////////////////
// READER

func (s *Statement) Scan(row pg.Row) error {
	return row.Scan(
		&s.Role, &s.Database, &s.QueryID, &s.Query,
		&s.Calls, &s.Rows,
		&s.Total, &s.Min, &s.Max, &s.Mean,
	)
}

func (l *StatementList) Scan(row pg.Row) error {
	var statement Statement
	if err := statement.Scan(row); err != nil {
		return err
	}
	l.Body = append(l.Body, statement)
	return nil
}

func (l *StatementList) ScanCount(row pg.Row) error {
	return row.Scan(&l.Count)
}

///////////////////////////////////////////////////////////////////////////////
// QUERIES

// pg_stat_statements query - joins with pg_roles and pg_database for names
const statementSelect = `
	SELECT
		COALESCE(u.rolname, '') AS role,
		COALESCE(d.datname, '') AS database,
		s.queryid,
		s.query,
		s.calls,
		s.rows,
		s.total_exec_time,
		s.min_exec_time,
		s.max_exec_time,
		s.mean_exec_time
	FROM
		public.pg_stat_statements s
	LEFT JOIN
		pg_catalog.pg_roles u ON s.userid = u.oid
	LEFT JOIN
		pg_catalog.pg_database d ON s.dbid = d.oid
`

const statementList = `WITH q AS (` + statementSelect + `) SELECT * FROM q ${where} ${orderby}`
