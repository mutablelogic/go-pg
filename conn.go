package pg

import (
	"context"
	"errors"

	// Packages
	pgx "github.com/jackc/pgx/v5"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Conn interface {
	// Return a new connection with bound parameters
	With(...any) Conn

	// Return a new connection with bound queries
	WithQueries(...*Queries) Conn

	// Return a connection to a remote database
	Remote(database string) Conn

	// Perform a transaction within a function
	Tx(context.Context, func(Conn) error) error

	// Perform a bulk operation within a function (and indicate whether this
	// should be in a transaction)
	Bulk(context.Context, func(Conn) error) error

	// Execute a query
	Exec(context.Context, string) error

	// Perform an insert
	Insert(context.Context, Reader, Writer) error

	// Perform an update
	Update(context.Context, Reader, Selector, Writer) error

	// Perform a delete
	Delete(context.Context, Reader, Selector) error

	// Perform a get
	Get(context.Context, Reader, Selector) error

	// Perform a list. If the reader is a ListReader, then the
	// count of items is also calculated
	List(context.Context, Reader, Selector) error
}

// Op represents a database operation type.
type Op uint

// Row is a pgx.Row for scanning query results.
type Row pgx.Row

// Reader scans a database row into an object.
type Reader interface {
	// Scan row into a result
	Scan(Row) error
}

// ListReader scans database rows and counts total results.
type ListReader interface {
	Reader

	// Scan count into the result
	ScanCount(Row) error
}

// Writer binds object fields for insert or update operations.
type Writer interface {
	// Set bind parameters for an insert
	Insert(*Bind) (string, error)

	// Set bind parameters for an update
	Update(*Bind) error
}

// Selector binds parameters for get, update, or delete operations.
type Selector interface {
	// Set bind parameters for getting, updating or deleting
	Select(*Bind, Op) (string, error)
}

// Concrete connection
type conn struct {
	conn pgx.Tx
	bind *Bind
}

// Ensure interfaces are satisfied
var _ Conn = (*conn)(nil)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// Operations
const (
	None Op = iota
	Get
	Insert
	Update
	Delete
	List
)

func (o Op) String() string {
	switch o {
	case Get:
		return "GET"
	case Insert:
		return "INSERT"
	case Update:
		return "UPDATE"
	case Delete:
		return "DELETE"
	case List:
		return "LIST"
	}
	return "UNKNOWN"
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - CONN

// Return a new connection with new bound parameters
func (p *conn) With(params ...any) Conn {
	return &conn{p.conn, p.bind.Copy(params...)}
}

// Return a new connection with bound queries
func (p *conn) WithQueries(queries ...*Queries) Conn {
	return &conn{p.conn, p.bind.withQueries(queries...)}
}

// Return a connection to a remote database
func (p *conn) Remote(database string) Conn {
	return &conn{p.conn, p.bind.withRemote(database)}
}

// Perform a transaction, then commit or rollback
func (p *conn) Tx(ctx context.Context, fn func(Conn) error) error {
	return tx(ctx, p.conn, p.bind, fn)
}

// Perform a bulk operation and indicate whether this should be in
// a transaction
func (p *conn) Bulk(ctx context.Context, fn func(Conn) error) error {
	return bulk(ctx, p.conn, p.bind, fn)
}

// Execute a query
func (p *conn) Exec(ctx context.Context, query string) error {
	return p.bind.exec(ctx, p.conn, query)
}

// Perform an insert, binding parameters from
// the writer, and scanning the result into the reader
func (p *conn) Insert(ctx context.Context, reader Reader, writer Writer) error {
	return insert(ctx, p.conn, p.bind, reader, writer)
}

// Perform an update, selecting using the selector, binding parameters from
// the writer, and scanning the result into the reader
func (p *conn) Update(ctx context.Context, reader Reader, sel Selector, writer Writer) error {
	return update(ctx, p.conn, p.bind, reader, sel, writer)
}

// Perform a delete, binding parameters with the selector and scanning the
// deleted data into the reader
func (p *conn) Delete(ctx context.Context, reader Reader, sel Selector) error {
	return del(ctx, p.conn, p.bind, reader, sel)
}

// Perform a get, binding parameters with the selector and scanning a single
// row into the reader
func (p *conn) Get(ctx context.Context, reader Reader, sel Selector) error {
	return get(ctx, p.conn, p.bind, reader, sel)
}

// Perform a list, binding parameters with the selector and scanning rows
// into the reader
func (p *conn) List(ctx context.Context, reader Reader, sel Selector) error {
	return list(ctx, p.conn, p.bind, reader, sel)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func tx(ctx context.Context, tx pgx.Tx, bind *Bind, fn func(Conn) error) error {
	tx, err := tx.Begin(ctx)
	if err != nil {
		return err
	}

	tx_ := &conn{tx, bind.Copy()}
	if err := fn(tx_); err != nil {
		return errors.Join(pgerror(err), tx.Rollback(ctx))
	} else {
		return errors.Join(pgerror(err), tx.Commit(ctx))
	}
}

func insert(ctx context.Context, conn pgx.Tx, bind *Bind, reader Reader, writer Writer) error {
	query, err := writer.Insert(bind)
	if err != nil {
		return err
	}
	return exec(ctx, conn, bind, query, reader)
}

func update(ctx context.Context, conn pgx.Tx, bind *Bind, reader Reader, sel Selector, writer Writer) error {
	query, err := sel.Select(bind, Update)
	if err != nil {
		return err
	}
	if writer != nil {
		if err := writer.Update(bind); err != nil {
			return err
		}
	}
	return exec(ctx, conn, bind, query, reader)
}

func del(ctx context.Context, conn pgx.Tx, bind *Bind, reader Reader, sel Selector) error {
	query, err := sel.Select(bind, Delete)
	if err != nil {
		return err
	}
	return exec(ctx, conn, bind, query, reader)
}

func get(ctx context.Context, conn pgx.Tx, bind *Bind, reader Reader, sel Selector) error {
	query, err := sel.Select(bind, Get)
	if err != nil {
		return err
	}
	return exec(ctx, conn, bind, query, reader)
}

func list(ctx context.Context, conn pgx.Tx, bind *Bind, reader Reader, sel Selector) error {
	bind.Set("offsetlimit", "")
	query, err := sel.Select(bind, List)
	if err != nil {
		return pgerror(err)
	}

	// Count the number of rows if the reader is a ListReader
	if counter, ok := reader.(ListReader); ok {
		if err := count(ctx, conn, query, bind, counter); err != nil {
			return pgerror(err)
		}
	}

	// Execute the query. In the case of a list, we return success even if there aren't any
	// rows returned
	if err := exec(ctx, conn, bind, query+` ${offsetlimit}`, reader); errors.Is(err, ErrNotFound) {
		return nil
	} else {
		return pgerror(err)
	}
}

func count(ctx context.Context, conn pgx.Tx, query string, bind *Bind, reader ListReader) error {
	// Make a subquery
	return pgerror(reader.ScanCount(bind.Copy("as", "t (count BIGINT)").queryRow(ctx, conn, `WITH sq AS (`+query+`) SELECT COUNT(*) AS "count" FROM sq`)))
}

func exec(ctx context.Context, conn pgx.Tx, bind *Bind, query string, reader Reader) error {
	// Without a reader, just execute the query
	if reader == nil {
		return pgerror(bind.exec(ctx, conn, query))
	}
	// Execute the query
	rows, err := bind.query(ctx, conn, query)
	if err != nil {
		return pgerror(err)
	}
	defer rows.Close()

	// Read rows
	var scanned bool
	for rows.Next() {
		if err := reader.Scan(rows); err != nil {
			return pgerror(err)
		}
		scanned = true
	}

	if err := rows.Err(); err != nil {
		return err
	} else if !scanned {
		return pgerror(pgx.ErrNoRows)
	}

	// Return success
	return nil
}
