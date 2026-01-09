package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"strings"
	"sync"

	// Packages
	pgx "github.com/jackc/pgx/v5"
	types "github.com/mutablelogic/go-pg/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Bind represents a set of variables and arguments to be used in a query.
// The vars are substituted in the query string itself, while the args are
// passed as arguments to the query.
type Bind struct {
	sync.RWMutex
	vars   pgx.NamedArgs
	dblink string // Used when executing transactions remotely
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewBind creates a new Bind object with the given name/value pairs.
// Returns nil if the number of arguments is not even.
func NewBind(pairs ...any) *Bind {
	if len(pairs)%2 != 0 {
		return nil
	}

	// Populate the vars map
	vars := make(pgx.NamedArgs, len(pairs)>>1)
	for i := 0; i < len(pairs); i += 2 {
		if key, ok := pairs[i].(string); !ok || key == "" {
			return nil
		} else {
			vars[key] = pairs[i+1]
		}
	}

	// Return the Bind object
	return &Bind{vars: vars}
}

// Copy creates a copy of the bind object with additional name/value pairs.
func (bind *Bind) Copy(pairs ...any) *Bind {
	if len(pairs)%2 != 0 {
		return nil
	}

	// Lock before copying
	varsCopy := func() pgx.NamedArgs {
		bind.RLock()
		defer bind.RUnlock()
		c := make(pgx.NamedArgs, len(bind.vars)+(len(pairs)>>1))
		maps.Copy(c, bind.vars)
		return c
	}()

	for i := 0; i < len(pairs); i += 2 {
		if key, ok := pairs[i].(string); !ok || key == "" {
			return nil
		} else {
			varsCopy[key] = pairs[i+1]
		}
	}

	// Return the copied Bind object
	return &Bind{vars: varsCopy, dblink: bind.dblink}
}

// Return a new bind object with the given database link
func (bind *Bind) withRemote(database string) *Bind {
	varsCopy := make(pgx.NamedArgs, len(bind.vars))
	maps.Copy(varsCopy, bind.vars)

	// Return the copied Bind object
	return &Bind{vars: varsCopy, dblink: "dbname=" + types.Quote(database)}
}

// Return a new bind object with one or more sets of queries
func (bind *Bind) withQueries(queries ...*Queries) *Bind {
	if len(queries) == 0 {
		return bind
	}

	// Make a copy of the bind vars
	varsCopy := make(pgx.NamedArgs, len(bind.vars))
	maps.Copy(varsCopy, bind.vars)

	// Iterate through queries
	for _, q := range queries {
		for _, key := range q.Keys() {
			varsCopy[key] = q.Get(key)
		}
	}

	// Return the copied Bind object
	return &Bind{vars: varsCopy}
}

///////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (bind *Bind) MarshalJSON() ([]byte, error) {
	return json.Marshal(bind.vars)
}

func (bind *Bind) String() string {
	data, err := json.MarshalIndent(bind.vars, "", "  ")
	if err != nil {
		return err.Error()
	} else {
		return string(data)
	}
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Set sets a bind var and returns the parameter name.
func (bind *Bind) Set(key string, value any) string {
	bind.Lock()
	defer bind.Unlock()

	if key == "" {
		return ""
	}
	bind.vars[key] = value
	return "@" + key
}

// Get returns a bind var by key.
func (bind *Bind) Get(key string) any {
	bind.RLock()
	defer bind.RUnlock()
	return bind.vars[key]
}

// Has returns true if there is a bind var with the given key.
func (bind *Bind) Has(key string) bool {
	bind.RLock()
	defer bind.RUnlock()

	_, ok := bind.vars[key]
	return ok
}

// Del deletes a bind var.
func (bind *Bind) Del(key string) {
	bind.Lock()
	defer bind.Unlock()
	delete(bind.vars, key)
}

// Join joins a bind var with a separator when it is a
// []any and returns the result as a string. Returns
// an empty string if the key does not exist.
func (bind *Bind) Join(key, sep string) string {
	bind.RLock()
	defer bind.RUnlock()

	if value, ok := bind.vars[key]; !ok {
		return ""
	} else if v, ok := value.([]any); ok {
		str := make([]string, len(v))
		for i, value := range v {
			str[i] = fmt.Sprint(value)
		}
		return strings.Join(str, sep)
	} else {
		return fmt.Sprint(value)
	}
}

// Append appends a bind var to a list. Returns false if the key
// is not a list, or the value is not a list.
func (bind *Bind) Append(key string, value any) bool {
	bind.Lock()
	defer bind.Unlock()

	// Create a new list if it doesn't exist
	if _, ok := bind.vars[key]; !ok {
		bind.vars[key] = make([]any, 0, 5)
	}

	// Check type
	if _, ok := bind.vars[key].([]any); !ok {
		return false
	}

	// Append value
	bind.vars[key] = append(bind.vars[key].([]any), value)

	// Return success
	return true
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - QUERY

// QueryRow queries a single row and returns the result.
func (bind *Bind) QueryRow(ctx context.Context, conn pgx.Tx, query string) pgx.Row {
	bind.RLock()
	defer bind.RUnlock()

	// dblink version
	if bind.dblink != "" {
		// 'as' is used to define the column names
		var def string
		if bind.Has("as") {
			def = ` AS ` + bind.Get("as").(string)
		}
		// TODO: Attempt to unroll the @parameters in the query
		return conn.QueryRow(ctx, replace(dblinkSelect, pgx.NamedArgs{
			"conn":  bind.dblink,
			"query": bind.Replace(query),
			"as":    def,
		}))
	}

	// normal version
	return conn.QueryRow(ctx, bind.Replace(query), bind.vars)
}

// Query a set of rows and return the result
func (bind *Bind) Query(ctx context.Context, conn pgx.Tx, query string) (pgx.Rows, error) {
	bind.RLock()
	defer bind.RUnlock()

	// dblink version
	if bind.dblink != "" {
		// 'as' is used to define the column names
		var def string
		if bind.Has("as") {
			def = ` AS ` + bind.Get("as").(string)
		}
		return conn.Query(ctx, replace(dblinkSelect, pgx.NamedArgs{
			"conn":  bind.dblink,
			"query": bind.Replace(query),
			"as":    def,
		}))
	}

	// normal version
	return conn.Query(ctx, bind.Replace(query), bind.vars)
}

// Exec executes a query.
func (bind *Bind) Exec(ctx context.Context, conn pgx.Tx, query string) error {
	bind.RLock()
	defer bind.RUnlock()

	// dblink version
	if bind.dblink != "" {
		// TODO: Attempt to unroll the parameters
		_, err := conn.Exec(ctx, replace(dblinkExec, pgx.NamedArgs{
			"conn":  bind.dblink,
			"query": bind.Replace(query),
		}))
		return err
	}

	// normal version
	_, err := conn.Exec(ctx, bind.Replace(query), bind.vars)
	return err
}

// Queue a query - for bulk operations
func (bind *Bind) queuerow(batch *pgx.Batch, query string, reader Reader) {
	bind.RLock()
	defer bind.RUnlock()
	queuedquery := batch.Queue(bind.Replace(query), bind.vars)
	queuedquery.QueryRow(func(row pgx.Row) error {
		return reader.Scan(row)
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// Replace returns a query string with ${subtitution} replaced by the values:
//   - ${key} => value
//   - ${'key'} => 'value'
//   - ${"key"} => "value"
//   - $1 => $1
//   - $$ => $$
func (bind *Bind) Replace(query string) string {
	return replace(query, bind.vars)
}

func replace(query string, vars pgx.NamedArgs) string {
	fetch := func(key string) string {
		return fmt.Sprint(vars[key])
	}
	return os.Expand(query, func(key string) string {
		if key == "$" { // $$ => $$
			return "$$"
		}
		if types.IsNumeric(key) {
			return "$" + key // $1 => $1
		}
		if types.IsSingleQuoted(key) { // ${'key'} => 'value'
			// Special case where value is []string and single quote for IN (${key})
			key := strings.Trim(key, "'")
			value := vars[key]
			switch v := value.(type) {
			case []string:
				result := make([]string, len(v))
				for i, s := range v {
					result[i] = types.Quote(s)
				}
				return strings.Join(result, ",")
			default:
				return types.Quote(fetch(key))
			}
		}
		if types.IsDoubleQuoted(key) { // ${"key"} => "value"
			return types.DoubleQuote(fetch(strings.Trim(key, "\"")))
		}
		return fetch(key) // ${key} => value
	})
}

///////////////////////////////////////////////////////////////////////////////
// SQL

const (
	dblinkSelect = "SELECT * FROM dblink(${'conn'}, ${'query'}, true)${as}"
	dblinkExec   = "SELECT dblink_exec(${'conn'}, ${'query'}, true)"
)
