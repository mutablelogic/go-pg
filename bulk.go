package pg

import (
	"context"

	// Packages
	pgx "github.com/jackc/pgx/v5"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type bulkconn struct {
	conn  pgx.Tx
	batch pgx.Batch
	bind  *Bind
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Return a new connection with bound parameters
func (conn *bulkconn) With(params ...any) Conn {
	return &bulkconn{conn.conn, conn.batch, conn.bind.Copy(params...)}
}

// Return a new connection with bound queries
func (conn *bulkconn) WithQueries(queries ...*Queries) Conn {
	return &bulkconn{conn.conn, conn.batch, conn.bind.withQueries(queries...)}
}

// Return nil for remote bulk connections
func (conn *bulkconn) Remote(database string) Conn {
	return nil
}

// Perform a transaction within a function
func (conn *bulkconn) Tx(context.Context, func(Conn) error) error {
	return ErrNotImplemented
}

// Perform a bulk operation within a function (and indicate whether this
// should be in a transaction)
func (conn *bulkconn) Bulk(context.Context, func(Conn) error) error {
	return ErrNotImplemented
}

// Execute a query
func (conn *bulkconn) Exec(context.Context, string) error {
	return ErrNotImplemented
}

// Perform an insert
func (conn *bulkconn) Insert(ctx context.Context, reader Reader, writer Writer) error {
	if query, err := writer.Insert(conn.bind); err != nil {
		return err
	} else {
		conn.bind.Copy().queuerow(&conn.batch, query, reader)
	}
	return nil
}

// Perform an update
func (conn *bulkconn) Update(context.Context, Reader, Selector, Writer) error {
	return ErrNotImplemented
}

// Perform a delete
func (conn *bulkconn) Delete(context.Context, Reader, Selector) error {
	return ErrNotImplemented
}

// Perform a get
func (conn *bulkconn) Get(context.Context, Reader, Selector) error {
	return ErrNotImplemented
}

// Perform a list. If the reader is a ListReader, then the
// count of items is also calculated
func (conn *bulkconn) List(context.Context, Reader, Selector) error {
	return ErrNotImplemented
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func bulk(ctx context.Context, tx pgx.Tx, bind *Bind, fn func(Conn) error) error {
	tx_ := new(bulkconn)
	tx_.conn = tx
	tx_.bind = bind
	if err := fn(tx_); err != nil {
		return pgerror(err)
	}

	// Send the batch
	return tx_.conn.SendBatch(ctx, &tx_.batch).Close()
}
