package pg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

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

const subscriptionCleanupTimeout = 5 * time.Second

type Notification struct {
	Channel string
	Payload []byte
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

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func subscribe(ctx context.Context, pg *poolconn, channel string) (<-chan Notification, error) {
	channel = strings.TrimSpace(channel)
	if channel == "" {
		return nil, ErrBadParameter.With("channel is required")
	}

	listener := pg.Listener()
	if listener == nil {
		return nil, ErrNotAvailable.With("subscriptions are unavailable")
	}
	if err := listener.Listen(ctx, channel); err != nil {
		return nil, errors.Join(err, closeListener(listener))
	}

	runCtx, cancel := context.WithCancel(ctx)
	sub, err := pg.conn.addSubscription(cancel)
	if err != nil {
		cancel()
		return nil, errors.Join(err, cleanupListener(listener, channel))
	}
	notifyCh := make(chan Notification)

	go func() {
		defer close(notifyCh)
		defer sub.Done()
		defer pg.conn.removeSubscription(sub)
		defer cancel()
		defer func() {
			cleanupListener(listener, channel)
		}()

		for {
			n, err := listener.WaitForNotification(runCtx)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				return
			}
			select {
			case notifyCh <- *n:
			case <-runCtx.Done():
				return
			}
		}
	}()

	return notifyCh, nil
}

func cleanupListener(listener Listener, channel string) error {
	ctx, cancel := context.WithTimeout(context.Background(), subscriptionCleanupTimeout)
	defer cancel()

	err := listener.Unlisten(ctx, channel)
	return errors.Join(err, listener.Close(ctx))
}

func closeListener(listener Listener) error {
	ctx, cancel := context.WithTimeout(context.Background(), subscriptionCleanupTimeout)
	defer cancel()

	return listener.Close(ctx)
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
