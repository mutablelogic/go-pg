package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	// Packages
	pgxpool "github.com/jackc/pgx/v5/pgxpool"
	types "github.com/mutablelogic/go-pg/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Listener is an interface for listening to notifications
type Listener interface {
	// Listen to a topic
	Listen(context.Context, string) error

	// Unlisten from a topic
	Unlisten(context.Context, string) error

	// Wait for a notification and return it
	WaitForNotification(context.Context) (*Notification, error)

	// Free resources
	Close(context.Context) error
}

type listener struct {
	sync.Mutex
	pool *pgxpool.Pool
	conn *pgxpool.Conn
}

var _ Listener = (*listener)(nil)

type Notification struct {
	Channel string
	Payload []byte
}

type subscriptionGroup struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	closed bool
	once   sync.Once
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewListener return a Listener for the given pool. If pool is nil then
// return nil
func (pg *poolconn) Listener() Listener {
	l := new(listener)
	l.pool = pg.conn.Pool
	return l
}

func newSubscriptionGroup() *subscriptionGroup {
	ctx, cancel := context.WithCancel(context.Background())
	return &subscriptionGroup{ctx: ctx, cancel: cancel}
}

// Close the connection to the database
func (l *listener) Close(ctx context.Context) error {
	l.Lock()
	defer l.Unlock()

	if l.conn == nil {
		return nil
	}

	// Release below would take care of cleanup and potentially put the
	// connection back into rotation, but in case a Listen was invoked without a
	// subsequent Unlisten on the same topic, close the connection explicitly to
	// guarantee no other caller will receive a partially tainted connection.
	err := l.conn.Conn().Close(ctx)

	// Release the connection
	l.conn.Release()
	l.conn = nil

	// Return any errors
	return err
}

func (g *subscriptionGroup) Go(ctx context.Context, fn func(context.Context)) error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return ErrNotAvailable.With("subscriptions are closed")
	}
	g.wg.Add(1)
	parent := g.ctx
	g.mu.Unlock()

	go func() {
		defer g.wg.Done()
		runCtx, cancel := context.WithCancel(parent)
		stop := context.AfterFunc(ctx, cancel)
		defer stop()
		defer cancel()
		fn(runCtx)
	}()

	return nil
}

func (g *subscriptionGroup) Close() {
	g.once.Do(func() {
		g.mu.Lock()
		g.closed = true
		g.mu.Unlock()
		g.cancel()
		g.wg.Wait()
	})
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func subscribe(ctx context.Context, pg *poolconn, channel string, fn func(Notification) error) error {
	channel = strings.TrimSpace(channel)
	if channel == "" {
		return ErrBadParameter.With("channel is required")
	}
	if fn == nil {
		return ErrBadParameter.With("callback is required")
	}

	group := pg.conn.subscriptionGroup()
	if group == nil {
		return ErrNotAvailable.With("subscriptions are unavailable")
	}

	listener := pg.Listener()
	if listener == nil {
		return ErrNotAvailable.With("listener is nil")
	}
	if err := listener.Listen(ctx, channel); err != nil {
		return err
	}
	if err := group.Go(ctx, func(runCtx context.Context) {
		defer listener.Unlisten(context.Background(), channel)
		defer listener.Close(context.Background())

		for {
			n, err := listener.WaitForNotification(runCtx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				return
			}
			if err := fn(*n); err != nil {
				return
			}
		}
	}); err != nil {
		err = errors.Join(err, listener.Unlisten(context.Background(), channel))
		return errors.Join(err, listener.Close(context.Background()))
	}

	return nil
}

// Connect to the database, and listen to a topic
func (l *listener) Listen(ctx context.Context, topic string) error {
	l.Lock()
	defer l.Unlock()

	// Acquire a connection
	if l.conn == nil {
		conn, err := l.pool.Acquire(ctx)
		if err != nil {
			return err
		} else {
			l.conn = conn
		}
	}

	// Listen to the topic
	_, err := l.conn.Exec(ctx, "LISTEN "+types.DoubleQuote(topic))
	return err
}

// Unlisten issues an UNLISTEN from the supplied topic.
func (l *listener) Unlisten(ctx context.Context, topic string) error {
	l.Lock()
	defer l.Unlock()

	// Check if the connection is nil
	if l.conn == nil {
		return fmt.Errorf("connection is nil")
	}

	// Unlisten from a topic
	_, err := l.conn.Exec(ctx, "UNLISTEN "+types.DoubleQuote(topic))
	return err
}

// WaitForNotification blocks until receiving a notification and returns it.
func (l *listener) WaitForNotification(ctx context.Context) (*Notification, error) {
	l.Lock()
	// Check if the connection is valid
	if l.conn == nil || l.conn.Conn() == nil {
		l.Unlock()
		return nil, fmt.Errorf("connection is nil")
	}
	conn := l.conn.Conn()
	l.Unlock()

	// Wait for a notification (without holding the lock)
	n, err := conn.WaitForNotification(ctx)
	if err != nil {
		return nil, err
	}

	// Return the notification
	return &Notification{
		Channel: n.Channel,
		Payload: []byte(n.Payload),
	}, nil
}
