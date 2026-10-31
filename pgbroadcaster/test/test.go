package test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	broadcaster "github.com/mutablelogic/go-pg/pgbroadcaster"
	test "github.com/mutablelogic/go-pg/pkg/test"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	shared  *broadcaster.Manager
	pool    pg.PoolConn
	logs    logBuffer
	cancels cancelRegistry
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type cancelRegistry struct {
	mu      sync.Mutex
	cancels map[*testing.T]context.CancelFunc
}

func (r *cancelRegistry) Store(t *testing.T, cancel context.CancelFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancels == nil {
		r.cancels = make(map[*testing.T]context.CancelFunc)
	}
	r.cancels[t] = cancel
}

func (r *cancelRegistry) LoadAndDelete(t *testing.T) (context.CancelFunc, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancels == nil {
		return nil, false
	}
	cancel, ok := r.cancels[t]
	if ok {
		delete(r.cancels, t)
	}
	return cancel, ok
}

// logBuffer is the log of the shared manager, which can be written while
// it's read
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// Main initializes a PostgreSQL pool, and a manager, on a channel of its own,
// whose event loop runs, logging to the log which Logs returns, before
// running the tests. If PGBROADCASTER_TEST_DATABASE_URL is set, it connects to that
// server; otherwise it starts a PostgreSQL container.
func Main(m *testing.M, opts ...broadcaster.Opt) {
	if databaseURL := os.Getenv("PGBROADCASTER_TEST_DATABASE_URL"); databaseURL != "" {
		os.Exit(runWithDatabaseURL(m, databaseURL, opts...))
		return
	}

	test.Main(m, func(conn pg.PoolConn) (func(), error) {
		return startManager(conn, opts...)
	})
}

func startManager(conn pg.PoolConn, opts ...broadcaster.Opt) (func(), error) {
	manager, stop, err := start(conn, slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})), opts...)
	if err != nil {
		return nil, err
	}
	shared, pool = manager, conn
	return func() {
		shared, pool = nil, nil
		if err := stop(); err != nil {
			panic(err)
		}
	}, nil
}

func runWithDatabaseURL(m *testing.M, databaseURL string, opts ...broadcaster.Opt) int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pg.NewPool(ctx, pg.WithURL(databaseURL))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	if err := conn.Ping(ctx); err != nil {
		panic(err)
	}

	teardown, err := startManager(conn, opts...)
	if err != nil {
		panic(err)
	}
	defer teardown()

	return m.Run()
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Start creates a manager on a channel of its own, so the tests of a package
// don't hear those of others which run at the same time against the same
// database, with the options, which can still set the channel, and runs it,
// until the returned function stops it, once it's listening. It's for the
// tests of the modules which listen with a broadcaster.
func Start(conn pg.PoolConn, opts ...broadcaster.Opt) (*broadcaster.Manager, func() error, error) {
	return start(conn, slog.Default(), opts...)
}

// Conn returns the connection to the test database, for tests which create
// managers of their own
func Conn() pg.PoolConn {
	return pool
}

// Logs returns what the shared manager has logged
func Logs() string {
	return logs.String()
}

// Begin returns the shared test manager, which is listening, and a per-test
// context
func Begin(t *testing.T) (*broadcaster.Manager, context.Context) {
	t.Helper()
	if shared == nil {
		t.Fatal("test manager is not initialized; call test.Main from TestMain")
	}
	base := context.Background()
	baseCancel := func() {}
	if deadline, ok := t.Deadline(); ok {
		base, baseCancel = context.WithDeadline(base, deadline)
	}
	ctx, cancel := context.WithCancel(base)
	stop := func() {
		cancel()
		baseCancel()
	}
	cancels.Store(t, context.CancelFunc(stop))
	t.Cleanup(func() {
		if cancel, ok := cancels.LoadAndDelete(t); ok {
			cancel()
		}
	})
	return shared, ctx
}

// End releases the per-test context created by Begin
func End(t *testing.T) {
	t.Helper()
	if cancel, ok := cancels.LoadAndDelete(t); ok {
		cancel()
	}
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// start creates a manager on a channel of its own, and runs it, logging with
// the logger, until the returned function stops it, once it's listening
func start(conn pg.PoolConn, logger *slog.Logger, opts ...broadcaster.Opt) (*broadcaster.Manager, func() error, error) {
	channel := "test_" + strings.ToLower(rand.Text())
	manager, err := broadcaster.New(context.Background(), conn, append([]broadcaster.Opt{broadcaster.WithChannel(channel)}, opts...)...)
	if err != nil {
		return nil, nil, err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		defer close(runDone)
		runDone <- manager.Run(runCtx, logger)
	}()
	stop := func() error {
		cancel()
		return <-runDone
	}
	if err := waitListening(manager, runDone); err != nil {
		return nil, nil, errors.Join(err, stop())
	}
	return manager, stop, nil
}

// waitListening waits for the manager to be listening, or returns an error
// if its event loop stops, or it doesn't in time
func waitListening(manager *broadcaster.Manager, runDone <-chan error) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(30 * time.Second)
	for !manager.Listening() {
		select {
		case err := <-runDone:
			return errors.Join(errors.New("event loop stopped while starting"), err)
		case <-timeout:
			return errors.New("timed out waiting for the event loop to listen")
		case <-ticker.C:
		}
	}
	return nil
}
