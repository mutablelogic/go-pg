package pg

import (
	"context"
	"errors"
	"strings"

	// Packages
	pgx "github.com/jackc/pgx/v5"
	pgconn "github.com/jackc/pgx/v5/pgconn"
	pgxpool "github.com/jackc/pgx/v5/pgxpool"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type PoolConn interface {
	Conn

	// Acquire a connection and ping it
	Ping(context.Context) error

	// Release resources
	Close()

	// Reset the connection pool
	Reset()

	// Return a listener for the connection pool
	Listener() Listener
}

type pool struct {
	*pgxpool.Pool
}

type poolconn struct {
	conn *pool
	bind *Bind
}

// Ensure interfaces are satisfied
var _ pgx.Tx = (*pool)(nil)
var _ PoolConn = (*poolconn)(nil)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewPool creates a new connection pool to a PostgreSQL server.
func NewPool(ctx context.Context, opts ...Opt) (PoolConn, error) {
	o, err := apply(opts...)
	if err != nil {
		return nil, err
	}
	poolconfig, err := pgxpool.ParseConfig(o.Encode())
	if err != nil {
		return nil, err
	}

	// If there is a trace function, then set it
	if o.TraceFn != nil {
		poolconfig.ConnConfig.Tracer = NewTracer(o.TraceFn)

		// Output the connection parameters
		parts := map[string]string{}
		for _, part := range o.encode("password") {
			kv := strings.SplitN(part, "=", 2)
			parts[kv[0]] = kv[1]
		}
		o.TraceFn(ctx, "CONNECT", parts, nil)
	}

	// Create the connection pool
	p, err := pgxpool.NewWithConfig(ctx, poolconfig)
	if err != nil {
		return nil, err
	}

	// Wrap the connection pool as if it's a transaction
	return &poolconn{&pool{p}, o.bind}, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - POOL

func (p *pool) Commit(ctx context.Context) error {
	return errors.New("cannot commit a connection pool")
}

func (p *pool) Rollback(ctx context.Context) error {
	return errors.New("cannot rollback a connection pool")
}

func (p *pool) Conn() *pgx.Conn {
	return nil
}

func (p *pool) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}
}

func (p *pool) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("cannot prepare a connection pool")
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - POOLCONN

func (p *poolconn) Ping(ctx context.Context) error {
	return p.conn.Pool.Ping(ctx)
}

func (p *poolconn) Close() {
	p.conn.Pool.Close()
}

func (p *poolconn) Reset() {
	p.conn.Pool.Reset()
}

// Return a new connection with new bound parameters
func (p *poolconn) With(params ...any) Conn {
	return &poolconn{p.conn, p.bind.Copy(params...)}
}

// Return a new connection with new bound parameters
func (p *poolconn) WithQueries(queries ...*Queries) Conn {
	return &poolconn{p.conn, p.bind.withQueries(queries...)}
}

// Return a new connection to a remote database
func (p *poolconn) Remote(database string) Conn {
	return &poolconn{p.conn, p.bind.withRemote(database)}
}

// Perform a transaction, then commit or rollback
func (p *poolconn) Tx(ctx context.Context, fn func(conn Conn) error) error {
	return tx(ctx, p.conn, p.bind, fn)
}

// Perform a bulk operation
func (p *poolconn) Bulk(ctx context.Context, fn func(conn Conn) error) error {
	return bulk(ctx, p.conn, p.bind, fn)
}

// Execute a query
func (p *poolconn) Exec(ctx context.Context, query string) error {
	return p.bind.Exec(ctx, p.conn, query)
}

// Perform an insert
func (p *poolconn) Insert(ctx context.Context, reader Reader, writer Writer) error {
	return insert(ctx, p.conn, p.bind, reader, writer)
}

// Perform a update
func (p *poolconn) Update(ctx context.Context, reader Reader, sel Selector, writer Writer) error {
	return update(ctx, p.conn, p.bind, reader, sel, writer)
}

// Perform a delete
func (p *poolconn) Delete(ctx context.Context, reader Reader, sel Selector) error {
	return del(ctx, p.conn, p.bind, reader, sel)
}

// Perform a get
func (p *poolconn) Get(ctx context.Context, reader Reader, sel Selector) error {
	return get(ctx, p.conn, p.bind, reader, sel)
}

// Perform a list
func (p *poolconn) List(ctx context.Context, reader Reader, sel Selector) error {
	return list(ctx, p.conn, p.bind, reader, sel)
}
