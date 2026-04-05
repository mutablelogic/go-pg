package pg

import (
	"context"
	"errors"
	"strings"
	"sync"

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
	mu            sync.Mutex
	subscriptions *subscriptionArray
	closeOnce     sync.Once
	closeDone     chan struct{}
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

	// If there is a trace function or OTEL tracer, wire it up
	if o.tracer.TraceFn != nil || o.tracer.otel != nil {
		// Copy into a standalone allocation so the opt struct (which holds
		// connection credentials) can be garbage-collected after NewPool returns.
		t := &tracer{TraceFn: o.tracer.TraceFn, otel: o.tracer.otel}
		poolconfig.ConnConfig.Tracer = t

		if t.TraceFn != nil {
			// Output the connection parameters
			parts := map[string]string{}
			for _, part := range o.encode("password") {
				kv := strings.SplitN(part, "=", 2)
				parts[kv[0]] = kv[1]
			}
			t.TraceFn(ctx, "CONNECT", parts, nil)
		}
	}

	// Create the connection pool
	p, err := pgxpool.NewWithConfig(ctx, poolconfig)
	if err != nil {
		return nil, err
	}

	// Wrap the connection pool as if it's a transaction
	return &poolconn{&pool{Pool: p, subscriptions: newSubscriptionArray(), closeDone: make(chan struct{})}, o.bind}, nil
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
	p.conn.close()
}

func (p *poolconn) Reset() {
	p.conn.reset()
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
	return tx(ctx, p.conn, p.bind.Copy(), fn)
}

// Perform a bulk operation
func (p *poolconn) Bulk(ctx context.Context, fn func(conn Conn) error) error {
	return bulk(ctx, p.conn, p.bind.Copy(), fn)
}

// Subscribe to a PostgreSQL notification channel using a dedicated connection.
// Subscribe returns only initial registration errors; runtime listener errors
// terminate the background subscription and close the returned channel.
func (p *poolconn) Subscribe(ctx context.Context, channel string) (<-chan Notification, error) {
	return subscribe(ctx, p, channel)
}

// Execute a query
func (p *poolconn) Exec(ctx context.Context, query string) error {
	return p.bind.Copy().exec(ctx, p.conn, query)
}

// Perform an insert
func (p *poolconn) Insert(ctx context.Context, reader Reader, writer Writer) error {
	return insert(ctx, p.conn, p.bind.Copy(), reader, writer)
}

// Perform a update
func (p *poolconn) Update(ctx context.Context, reader Reader, sel Selector, writer Writer) error {
	return update(ctx, p.conn, p.bind.Copy(), reader, sel, writer)
}

// Perform a delete
func (p *poolconn) Delete(ctx context.Context, reader Reader, sel Selector) error {
	return del(ctx, p.conn, p.bind.Copy(), reader, sel)
}

// Perform a get
func (p *poolconn) Get(ctx context.Context, reader Reader, sel Selector) error {
	return get(ctx, p.conn, p.bind.Copy(), reader, sel)
}

// Perform a list
func (p *poolconn) List(ctx context.Context, reader Reader, sel Selector) error {
	return list(ctx, p.conn, p.bind.Copy(), reader, sel)
}

func (p *pool) addSubscription(cancel context.CancelFunc) (*subscription, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.subscriptions == nil {
		return nil, ErrNotAvailable.With("subscriptions are unavailable")
	}

	return p.subscriptions.Add(cancel)
}

func (p *pool) removeSubscription(sub *subscription) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.subscriptions != nil {
		p.subscriptions.Remove(sub)
	}
}

func (p *pool) swapSubscriptions(next *subscriptionArray) *subscriptionArray {
	p.mu.Lock()
	current := p.subscriptions
	p.subscriptions = next
	p.mu.Unlock()
	return current
}

func (p *pool) close() {
	p.closeOnce.Do(func() {
		p.closeSubscriptions(p.swapSubscriptions(nil))
		p.Pool.Close()
		close(p.closeDone)
	})
	<-p.closeDone
}

func (p *pool) reset() {
	p.closeSubscriptions(p.swapSubscriptions(newSubscriptionArray()))
	p.Pool.Reset()
}

func (p *pool) closeSubscriptions(group *subscriptionArray) {
	if group != nil {
		for _, sub := range group.Close() {
			sub.Wait()
		}
	}
}
