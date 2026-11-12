package schema

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	tui "github.com/mutablelogic/go-server/pkg/tui"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type ConnectionPid uint64

type Connection struct {
	Pid         uint32    `json:"pid" help:"Process ID"`
	Database    string    `json:"database" help:"Database"`
	Role        string    `json:"role" help:"Role"`
	Application *string   `json:"application,omitempty" help:"Application"`
	ClientAddr  string    `json:"client_addr,omitempty" help:"Client address"`
	ClientPort  uint16    `json:"client_port,omitempty" help:"Client port"`
	ConnStart   time.Time `json:"conn_start,omitempty" help:"Connection start"`
	QueryStart  time.Time `json:"query_start,omitempty" help:"Query start"`
	Query       string    `json:"query,omitempty" help:"Query"`
	State       string    `json:"state,omitempty" help:"State"`
}

type ConnectionListRequest struct {
	pg.OffsetLimit
	Database *string `json:"database,omitempty" help:"Database"`
	Role     *string `json:"role,omitempty" help:"Role"`
	State    *string `json:"state,omitempty" help:"State"`

	// Sort is what the connections are sorted by, which is the pid when it's
	// empty, and Order its direction, asc or desc, which is asc when it's
	// empty
	Sort  string `json:"sort,omitempty" help:"Sort by database, role, application, client, state, conn_start or query_start"`
	Order string `json:"order,omitempty" help:"Order of the sort (asc or desc)"`
}

type ConnectionList struct {
	ConnectionListRequest
	Count uint64       `json:"count"`
	Body  []Connection `json:"body,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (c Connection) String() string {
	return types.Stringify(c)
}

func (c ConnectionListRequest) String() string {
	return types.Stringify(c)
}

func (c ConnectionList) String() string {
	return types.Stringify(c)
}

////////////////////////////////////////////////////////////////////////////////
// QUERY

func (q ConnectionListRequest) Query() url.Values {
	values := url.Values{}
	if q.Offset > 0 {
		values.Set("offset", fmt.Sprint(q.Offset))
	}
	if q.Limit != nil {
		values.Set("limit", fmt.Sprint(types.Value(q.Limit)))
	}
	if q.Database != nil {
		values.Set("database", types.Value(q.Database))
	}
	if q.Role != nil {
		values.Set("role", types.Value(q.Role))
	}
	if q.State != nil {
		values.Set("state", types.Value(q.State))
	}
	if q.Sort != "" {
		values.Set("sort", q.Sort)
	}
	if q.Order != "" {
		values.Set("order", q.Order)
	}
	return values
}

////////////////////////////////////////////////////////////////////////////////
// TABLE

func (r Connection) Header() []string {
	return []string{"Pid", "Database", "Role", "Application", "Client Addr", "Client Port", "Conn Start", "Query Start", "Query", "State"}
}

func (r Connection) Width(col int) int {
	return 0
}

func (r Connection) Cell(col int) tui.CellText {
	switch col {
	case 0:
		return fmt.Sprint(r.Pid)
	case 1:
		return r.Database
	case 2:
		return r.Role
	case 3:
		return types.Value(r.Application)
	case 4:
		return r.ClientAddr
	case 5:
		return fmt.Sprint(r.ClientPort)
	case 6:
		return r.ConnStart.Format(time.RFC3339)
	case 7:
		return r.QueryStart.Format(time.RFC3339)
	case 8:
		return r.Query
	case 9:
		return r.State
	default:
		return ""
	}
}

////////////////////////////////////////////////////////////////////////////////
// SELECT

func (c *ConnectionListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Where
	bind.Del("where")
	if c.Database != nil {
		bind.Append("where", `"database" = `+bind.Set("database", strings.TrimSpace(types.Value(c.Database))))
	}
	if c.Role != nil {
		bind.Append("where", `"role" = `+bind.Set("role", strings.TrimSpace(types.Value(c.Role))))
	}
	if c.State != nil {
		bind.Append("where", `"state" = `+bind.Set("state", strings.TrimSpace(types.Value(c.State))))
	}
	if where := bind.Join("where", " AND "); where != "" {
		bind.Set("where", `WHERE `+where)
	} else {
		bind.Set("where", "")
	}

	// Order, by the sort, and then by the pid, so the pages are stable
	var column string
	switch strings.ToLower(c.Sort) {
	case "":
		column = `"pid"`
	case "database", "role", "application", "state", "conn_start", "query_start":
		column = `"` + strings.ToLower(c.Sort) + `"`
	case "client":
		column = `"client_addr"`
	default:
		return "", pg.ErrBadParameter.Withf("invalid sort parameter %q", c.Sort)
	}
	var direction string
	switch strings.ToLower(c.Order) {
	case "", "asc":
		direction = "ASC"
	case "desc":
		direction = "DESC"
	default:
		return "", pg.ErrBadParameter.Withf("invalid order parameter %q", c.Order)
	}
	orderby := "ORDER BY " + column + " " + direction + " NULLS LAST"
	if column != `"pid"` {
		orderby += `, "pid" ASC`
	}
	bind.Set("orderby", orderby)

	// Offset and limit
	c.OffsetLimit.Bind(bind, ConnectionListLimit)

	// Return query
	switch op {
	case pg.List:
		return connectionList, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported ConnectionListRequest operation %q", op)
	}
}

func (c ConnectionPid) Select(bind *pg.Bind, op pg.Op) (string, error) {
	if c == 0 {
		return "", pg.ErrBadParameter.With("missing pid")
	} else {
		bind.Set("pid", c)
	}

	// Return query
	switch op {
	case pg.Get:
		return connectionGet, nil
	case pg.Delete:
		return connectionDelete, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported ConnectionPid operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// READER

// Scan reads a connection. A connection by a Unix socket has no client
// address or port, and one which hasn't run a query has no query start.
func (c *Connection) Scan(row pg.Row) error {
	var result bool
	var connStart, queryStart *time.Time
	if err := row.Scan(&c.Pid, &c.Database, &c.Role, &c.Application, &c.ClientAddr, &c.ClientPort, &connStart, &queryStart, &c.Query, &c.State, &result); err != nil {
		return err
	}
	c.ConnStart = types.Value(connStart)
	c.QueryStart = types.Value(queryStart)
	return nil
}

func (c *ConnectionList) Scan(row pg.Row) error {
	var connection Connection
	if err := connection.Scan(row); err != nil {
		return err
	} else {
		c.Body = append(c.Body, connection)
	}
	return nil
}

func (c *ConnectionList) ScanCount(row pg.Row) error {
	return row.Scan(&c.Count)
}

////////////////////////////////////////////////////////////////////////////////
// SQL

const (
	connectionSelect = `
		WITH conn AS (
			SELECT
				C.pid AS "pid", 
				C.datname AS "database",
				C.usename AS "role",
				NULLIF(C.application_name, '') AS "application",
				COALESCE(C.client_hostname, abbrev(C.client_addr), '') AS "client_addr",
				COALESCE(NULLIF(C.client_port, -1), 0) AS "client_port",
				C.backend_start AS "conn_start",
				C.query_start AS "query_start",
				C.query AS "query",
				C.state AS "state"
			FROM
				${"schema"}."pg_stat_activity" C
			WHERE
				C.datname IS NOT NULL
			AND
				C.state IS NOT NULL
		) SELECT * FROM conn`
	connectionGet    = `WITH q AS (` + connectionSelect + `) SELECT *, false FROM q WHERE "pid" = @pid`
	connectionList   = `WITH q AS (` + connectionSelect + `) SELECT *, false FROM q ${where} ${orderby}`
	connectionDelete = `WITH q AS (` + connectionSelect + `) SELECT *, pg_terminate_backend(${pid}) FROM q WHERE pid <> pg_backend_pid()`
)
