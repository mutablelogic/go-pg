package queue_test

import (
	"context"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	test "github.com/mutablelogic/go-pg/pkg/test"
	assert "github.com/stretchr/testify/assert"
)

// Global connection variable
var conn test.Conn

// Start up a container and test the pool
func TestMain(m *testing.M) {
	test.Main(m, &conn)
}

////////////////////////////////////////////////////////////////////////////////
// MANAGER LIFECYCLE TESTS

func Test_Manager_New(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	t.Run("ValidConnection", func(t *testing.T) {
		mgr, err := queue.New(context.TODO(), conn, queue.WithNamespace("test"))
		assert.NoError(err)
		assert.NotNil(mgr)
	})

	t.Run("NilConnection", func(t *testing.T) {
		_, err := queue.New(context.TODO(), nil, queue.WithNamespace("test"))
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("EmptyNamespace", func(t *testing.T) {
		_, err := queue.New(context.TODO(), conn, queue.WithNamespace(""))
		assert.Error(err)
		assert.ErrorIs(err, queue.ErrInvalidNamespace)
	})

	t.Run("DifferentNamespaces", func(t *testing.T) {
		mgr1, err := queue.New(context.TODO(), conn, queue.WithNamespace("namespace1"))
		assert.NoError(err)
		assert.NotNil(mgr1)

		mgr2, err := queue.New(context.TODO(), conn, queue.WithNamespace("namespace2"))
		assert.NoError(err)
		assert.NotNil(mgr2)

		// Both managers should be valid but independent
		assert.NotEqual(mgr1, mgr2)
	})

	t.Run("ReservedNamespace", func(t *testing.T) {
		_, err := queue.New(context.TODO(), conn, queue.WithNamespace("pgqueue"))
		assert.Error(err)
		assert.ErrorIs(err, queue.ErrReservedNamespace)
	})
}

func Test_Manager_Run(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_run"))
	assert.NoError(err)
	assert.NotNil(mgr)

	// Create a test queue
	_, err = mgr.RegisterQueue(ctx, schema.QueueMeta{
		Queue: "test-cleanup-queue",
	})
	assert.NoError(err)

	// Start Run in background with a short-lived context
	runCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- mgr.Run(runCtx)
	}()

	// Wait for context to timeout
	select {
	case err := <-errCh:
		// Should return nil when context is cancelled
		assert.NoError(err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after context cancellation")
	}
}

func Test_Manager_WithSchemaSearchPath(t *testing.T) {
	assert := assert.New(t)
	ctx := context.TODO()

	// Create a new container and pool with schema search path
	container, pool, err := test.NewPgxContainer(ctx, "queue.schema.test", testing.Verbose(), func(ctx context.Context, sql string, args any, err error) {
		if err != nil {
			t.Logf("ERROR: %v", err)
		}
		if testing.Verbose() || err != nil {
			if args == nil {
				t.Logf("SQL: %v", sql)
			} else {
				t.Logf("SQL: %v, ARGS: %v", sql, args)
			}
		}
	}, "pgqueue", "public")
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	t.Run("WithSearchPath", func(t *testing.T) {
		mgr, err := queue.New(ctx, pool, queue.WithNamespace("test_with_search_path"))
		assert.NoError(err)
		assert.NotNil(mgr)
	})

	t.Run("MultipleNamespaces", func(t *testing.T) {
		mgr1, err := queue.New(ctx, pool, queue.WithNamespace("ns1"))
		assert.NoError(err)
		assert.NotNil(mgr1)

		mgr2, err := queue.New(ctx, pool, queue.WithNamespace("ns2"))
		assert.NoError(err)
		assert.NotNil(mgr2)

		// Both managers should work with same schema search path
		assert.NotEqual(mgr1, mgr2)
	})
}
