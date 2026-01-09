package queue_test

import (
	"context"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	queue "github.com/mutablelogic/go-pg/pkg/queue"
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
		mgr, err := queue.New(context.TODO(), conn, "test")
		assert.NoError(err)
		assert.NotNil(mgr)
	})

	t.Run("NilConnection", func(t *testing.T) {
		_, err := queue.New(context.TODO(), nil, "test")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("EmptyNamespace", func(t *testing.T) {
		mgr, err := queue.New(context.TODO(), conn, "")
		assert.NoError(err)
		assert.NotNil(mgr)
	})

	t.Run("DifferentNamespaces", func(t *testing.T) {
		mgr1, err := queue.New(context.TODO(), conn, "namespace1")
		assert.NoError(err)
		assert.NotNil(mgr1)

		mgr2, err := queue.New(context.TODO(), conn, "namespace2")
		assert.NoError(err)
		assert.NotNil(mgr2)

		// Both managers should be valid but independent
		assert.NotEqual(mgr1, mgr2)
	})
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
		mgr, err := queue.New(ctx, pool, "test_with_search_path")
		assert.NoError(err)
		assert.NotNil(mgr)
	})

	t.Run("MultipleNamespaces", func(t *testing.T) {
		mgr1, err := queue.New(ctx, pool, "ns1")
		assert.NoError(err)
		assert.NotNil(mgr1)

		mgr2, err := queue.New(ctx, pool, "ns2")
		assert.NoError(err)
		assert.NotNil(mgr2)

		// Both managers should work with same schema search path
		assert.NotEqual(mgr1, mgr2)
	})
}
