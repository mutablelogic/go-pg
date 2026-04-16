package test

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pkg/manager"
	types "github.com/mutablelogic/go-server/pkg/types"
)

/////////////////////////////////////////////////////////////////////
// TYPES

// Conn is a wrapper around pg.PoolConn which provides a test connection
type Conn struct {
	pg.PoolConn
	t *testing.T
}

/////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	timeout = 2 * time.Minute
)

/////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Main is the entry point for running tests with a test container. It sets up a test database container
// and runs the tests in the provided testing.M. The provided setup function can be used to perform any additional
// setup before running the tests, such as initializing the database schema.
// The returned cleanup function from setup will be called after the tests are complete to clean up any resources.
func Main(m *testing.M, setup func(pg.PoolConn) (func(), error)) {
	os.Exit(main(m, setup))
}

func main(m *testing.M, setup func(pg.PoolConn) (func(), error)) int {
	// Context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Name of executable
	name, err := os.Executable()
	if err != nil {
		panic(err)
	}

	// Start the container
	verbose := slices.Contains(os.Args, "-test.v=true")
	container, pool, err := NewPgxContainer(ctx, filepath.Base(name), verbose, func(ctx context.Context, sql string, args any, err error) {
		if err != nil {
			log.Printf("ERROR: %v", err)
		}
		if verbose || err != nil {
			if args == nil {
				log.Printf("SQL: %v", sql)
			} else {
				log.Printf("SQL: %v, ARGS: %v", sql, args)
			}
		}
	})
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	defer container.Close(ctx)

	// Run setup if provided
	cleanup := func() {}
	if setup != nil {
		cleanup_, err := setup(types.Ptr(Conn{pool, nil}))
		if err != nil {
			panic(err)
		} else if cleanup_ != nil {
			cleanup = cleanup_
		}
	}
	defer cleanup()

	// Run tests
	return m.Run()
}

// Begin a test
func (c *Conn) Begin(t *testing.T) pg.PoolConn {
	t.Log("Begin", t.Name())
	return &Conn{c.PoolConn, t}
}

// Close ends the test.
func (c *Conn) Close() {
	if c.t != nil {
		c.t.Log("End", c.t.Name())
	}
}

// ManagerConn wraps a Manager with its underlying connection for testing
type ManagerConn struct {
	*manager.Manager
	pool      pg.PoolConn
	container *Container
}

// NewManager creates a new Manager with a test container for integration testing.
// The returned ManagerConn must be closed after use.
func NewManager(t *testing.T) *ManagerConn {
	t.Helper()
	t.Log("Begin", t.Name())

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Start the container
	name, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	verbose := slices.Contains(os.Args, "-test.v=true")
	container, pool, err := NewPgxContainer(ctx, filepath.Base(name), verbose, func(ctx context.Context, sql string, args any, err error) {
		if err != nil {
			log.Printf("ERROR: %v", err)
		}
		if verbose || err != nil {
			if args == nil {
				log.Printf("SQL: %v", sql)
			} else {
				log.Printf("SQL: %v, ARGS: %v", sql, args)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create the manager
	mgr, err := manager.New(ctx, pool)
	if err != nil {
		pool.Close()
		container.Close(ctx)
		t.Fatal(err)
	}

	return &ManagerConn{
		Manager:   mgr,
		pool:      pool,
		container: container,
	}
}

// Close closes the manager connection and container
func (m *ManagerConn) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	m.pool.Close()
	m.container.Close(ctx)
}
