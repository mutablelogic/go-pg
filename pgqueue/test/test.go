package test

import (
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
	pgqueue "github.com/mutablelogic/go-pg/pgqueue"
	test "github.com/mutablelogic/go-pg/pkg/test"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	shared  *pgqueue.Manager
	cancels cancelRegistry

	// Database schema for the shared manager, which is dropped after the
	// tests have run
	sharedSchema string
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

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// Main initializes a PostgreSQL pool and manager before running the tests, and
// runs the manager's event loop while they run. If PGQUEUE_TEST_DATABASE_URL
// is set, it connects to that server; otherwise it starts a PostgreSQL
// container.
func Main(m *testing.M, setup func(*pgqueue.Manager) (func(), error), opts ...pgqueue.Opt) {
	if databaseURL := os.Getenv("PGQUEUE_TEST_DATABASE_URL"); databaseURL != "" {
		os.Exit(runWithDatabaseURL(m, databaseURL, setup, opts...))
		return
	}

	test.Main(m, func(pool pg.PoolConn) (func(), error) {
		return startManager(pool, setup, opts...)
	})
}

func startManager(pool pg.PoolConn, setup func(*pgqueue.Manager) (func(), error), opts ...pgqueue.Opt) (func(), error) {
	// Use a new database schema for each test process. Test packages can run
	// at the same time against the same database, and would otherwise share
	// queues and tasks. Options passed by the caller come afterwards, so can
	// still set the schema.
	sharedSchema = "test_" + strings.ToLower(rand.Text())
	schemaName := sharedSchema
	opts = append([]pgqueue.Opt{pgqueue.WithSchema(schemaName)}, opts...)

	// Drop the schema after the tests have run
	cleanup := func() error {
		sharedSchema = ""
		return pg.SchemaDrop(context.Background(), pool, schemaName)
	}

	manager, err := pgqueue.New(context.Background(), pool, opts...)
	if err != nil {
		return nil, errors.Join(err, cleanup())
	}

	teardown := func() {}
	if setup != nil {
		if teardown_, err := setup(manager); err != nil {
			return nil, errors.Join(err, cleanup())
		} else if teardown_ != nil {
			teardown = teardown_
		}
	}
	shared = manager

	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() {
		runDone <- manager.Run(runCtx, slog.Default())
	}()

	// Wait for the event loop to be running before any test starts
	if err := waitRunning(manager, runDone); err != nil {
		cancel()
		shared = nil
		teardown()
		return nil, errors.Join(err, cleanup())
	}

	return func() {
		cancel()
		runErr := <-runDone
		shared = nil
		teardown()
		if err := errors.Join(runErr, cleanup()); err != nil {
			panic(err)
		}
	}, nil
}

func runWithDatabaseURL(m *testing.M, databaseURL string, setup func(*pgqueue.Manager) (func(), error), opts ...pgqueue.Opt) int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pg.NewPool(ctx, pg.WithURL(databaseURL))
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		panic(err)
	}

	teardown, err := startManager(pool, setup, opts...)
	if err != nil {
		panic(err)
	}
	defer teardown()

	return m.Run()
}

// Schema returns the database schema used by the shared test manager, for
// tests which need to refer to its tables directly
func Schema() string {
	return sharedSchema
}

// IsolatedManager returns a new manager on a new, empty database schema, so
// that a test can make assertions about every object, which it cannot do with
// the shared manager as other tests also create objects. The manager's event
// loop is not run.
func IsolatedManager(t *testing.T, opts ...pgqueue.Opt) *pgqueue.Manager {
	t.Helper()
	if shared == nil {
		t.Fatal("test manager is not initialized; call test.Main from TestMain")
	}
	pool := shared.PoolConn

	// Create the manager on a new schema
	schemaName := "test_" + strings.ToLower(rand.Text())
	manager, err := pgqueue.New(context.Background(), pool, append(opts, pgqueue.WithSchema(schemaName))...)
	if err != nil {
		t.Fatal(err)
	}

	// Drop the schema when the test ends
	t.Cleanup(func() {
		if err := pg.SchemaDrop(context.Background(), pool, schemaName); err != nil {
			t.Error(err)
		}
	})

	// Return the manager
	return manager
}

// waitRunning waits for the manager's event loop to be running, or returns
// an error if it fails to start
func waitRunning(manager *pgqueue.Manager, runDone <-chan error) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(30 * time.Second)
	for !manager.Running() {
		select {
		case err := <-runDone:
			return errors.Join(errors.New("event loop stopped while starting"), err)
		case <-timeout:
			return errors.New("timed out waiting for the event loop to start")
		case <-ticker.C:
		}
	}
	return nil
}

// Begin returns the shared test manager and a per-test context.
func Begin(t *testing.T) (*pgqueue.Manager, context.Context) {
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

// End releases the per-test context created by Begin.
func End(t *testing.T) {
	t.Helper()
	if cancel, ok := cancels.LoadAndDelete(t); ok {
		cancel()
	}
}
