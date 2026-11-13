package manager_test

import (
	"context"
	"strings"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	harness "github.com/mutablelogic/go-pg/pgmanager/test"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// sleeping starts a query on a connection of the test's own, which sleeps,
// and returns its pid, once it's running, and what the query returns, when
// it ends
func sleeping(t *testing.T, ctx context.Context, mgr interface {
	ListConnections(context.Context, schema.ConnectionListRequest) (*schema.ConnectionList, error)
}, tag string) (uint64, <-chan error) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		done <- harness.Pool().Exec(context.Background(), "SELECT pg_sleep(30) /* "+tag+" */")
	}()
	for range 100 {
		list, err := mgr.ListConnections(ctx, schema.ConnectionListRequest{State: types.Ptr("active")})
		require.NoError(t, err)
		for _, conn := range list.Body {
			if strings.Contains(conn.Query, tag) {
				return uint64(conn.Pid), done
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("the query didn't start")
	return 0, nil
}

func TestConnectionCancelAndDelete(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	// A connection's query is cancelled, and the connection is returned, by
	// its pid, and stays connected
	pid, done := sleeping(t, ctx, mgr, "cancel-me")
	cancelled, err := mgr.CancelConnection(ctx, pid)
	require.NoError(t, err)
	assert.Equal(t, pid, uint64(cancelled.Pid))
	select {
	case err := <-done:
		assert.ErrorContains(t, err, "canceling statement due to user request")
	case <-time.After(10 * time.Second):
		t.Fatal("the query wasn't cancelled")
	}
	_, err = mgr.GetConnection(ctx, pid)
	assert.NoError(t, err)

	// A connection which is ended is returned, by its pid, and its query
	// ends with it
	pid, done = sleeping(t, ctx, mgr, "end-me")
	ended, err := mgr.DeleteConnection(ctx, pid)
	require.NoError(t, err)
	assert.Equal(t, pid, uint64(ended.Pid))
	select {
	case err := <-done:
		assert.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the connection wasn't ended")
	}

	// A pid which isn't a connection isn't found, rather than ending another
	_, err = mgr.DeleteConnection(ctx, 1)
	assert.ErrorIs(t, err, pg.ErrNotFound)
	_, err = mgr.CancelConnection(ctx, 1)
	assert.ErrorIs(t, err, pg.ErrNotFound)
}

// A connection whose role is dropped, while it's connected, has no role,
// and is still listed
func TestConnectionOfDroppedRole(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	// A connection, by dblink, as a role, which is then dropped
	require.NoError(t, harness.Pool().Exec(ctx, `CREATE EXTENSION IF NOT EXISTS dblink`))
	require.NoError(t, harness.Pool().Exec(ctx, `CREATE ROLE dropped_role LOGIN PASSWORD 'dropped'`))
	require.NoError(t, harness.Pool().Exec(ctx, `SELECT dblink_connect('dropped', 'host=localhost dbname=postgres user=dropped_role password=dropped')`))
	defer func() {
		_ = harness.Pool().Exec(context.Background(), `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename IS NULL AND datname IS NOT NULL`)
	}()
	require.NoError(t, harness.Pool().Exec(ctx, `DROP ROLE dropped_role`))

	// It's listed, without a role
	list, err := mgr.ListConnections(ctx, schema.ConnectionListRequest{})
	require.NoError(t, err)
	found := false
	for _, conn := range list.Body {
		if conn.Role == "" {
			found = true
		}
	}
	assert.True(t, found)
}
