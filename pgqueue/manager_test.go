package pgqueue_test

import (
	"context"
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

	t.Run("NoPool", func(t *testing.T) {
		_, err := pgqueue.New(context.Background(), nil)
		assert.ErrorIs(t, err, pg.ErrBadParameter)
	})
}
