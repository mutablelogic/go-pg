package pgqueue_test

import (
	"context"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	pgqueue "github.com/mutablelogic/go-pg/pgqueue"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	test "github.com/mutablelogic/go-pg/pgqueue/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	test.Main(m, nil)
}

func TestNew(t *testing.T) {
	manager, ctx := test.Begin(t)
	defer test.End(t)

	t.Run("Shared", func(t *testing.T) {
		// The shared manager's schema exists, and its event loop is running
		exists, err := pg.SchemaExists(ctx, manager, test.Schema())
		require.NoError(t, err)
		assert.True(t, exists)
		// The loop may be listening again, after another test resets the pool
		assert.Eventually(t, manager.Running, 10*time.Second, 10*time.Millisecond)
	})

	t.Run("Isolated", func(t *testing.T) {
		// An isolated manager has its own schema, and isn't running
		isolated := test.IsolatedManager(t)
		assert.NotSame(t, manager, isolated)
		assert.False(t, isolated.Running())
	})

	t.Run("Tickers", func(t *testing.T) {
		// A manager registers the maintenance and cleanup tickers
		isolated := test.IsolatedManager(t, pgqueue.WithMaintenancePeriod(time.Hour))
		maintenance, err := isolated.GetTicker(ctx, schema.DefaultMaintenanceTickerName)
		require.NoError(t, err)
		assert.Equal(t, time.Hour, *maintenance.Interval)
		cleanup, err := isolated.GetTicker(ctx, schema.DefaultCleanupTickerName)
		require.NoError(t, err)
		assert.Equal(t, schema.DefaultCleanupPeriod, *cleanup.Interval)
	})

	t.Run("TaskPriority", func(t *testing.T) {
		// A task table from before has no priority, and a manager adds it,
		// with a normal priority for the tasks, which can't be negative
		name := "test_" + strings.ToLower(rand.Text())
		_, err := pgqueue.New(ctx, manager.PoolConn, pgqueue.WithSchema(name))
		require.NoError(t, err)
		t.Cleanup(func() {
			assert.NoError(t, pg.SchemaDrop(context.Background(), manager.PoolConn, name))
		})
		conn := manager.PoolConn.With("schema", name)
		require.NoError(t, conn.Exec(ctx, `INSERT INTO ${"schema"}."queue" ("queue") VALUES ('priority')`))
		require.NoError(t, conn.Exec(ctx, `SELECT ${"schema"}.queue_insert('priority', '{}', NULL, NULL)`))
		require.NoError(t, conn.Exec(ctx, `ALTER TABLE ${"schema"}."task" DROP COLUMN "priority"`))

		// Create the manager again, on the schema from before
		_, err = pgqueue.New(ctx, manager.PoolConn, pgqueue.WithSchema(name))
		require.NoError(t, err)
		require.NoError(t, conn.Exec(ctx, `DO $$ BEGIN
			IF EXISTS (SELECT 1 FROM ${"schema"}."task" WHERE "priority" <> 0) OR NOT EXISTS (SELECT 1 FROM ${"schema"}."task") THEN
				RAISE EXCEPTION 'tasks from before have no normal priority';
			END IF;
		END $$`))
		assert.Error(t, conn.Exec(ctx, `UPDATE ${"schema"}."task" SET "priority" = -1`))
		assert.NoError(t, conn.Exec(ctx, `UPDATE ${"schema"}."task" SET "priority" = 10`))
	})

	t.Run("NoPool", func(t *testing.T) {
		_, err := pgqueue.New(context.Background(), nil)
		assert.ErrorIs(t, err, pg.ErrBadParameter)
	})
}
