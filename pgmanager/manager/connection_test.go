package manager_test

import (
	"sync"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	harness "github.com/mutablelogic/go-pg/pgmanager/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestConnectionCancelAndDelete(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	// The pool opens more than one connection, when it's used at once
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = mgr.ListConnections(ctx, schema.ConnectionListRequest{})
		}()
	}
	wg.Wait()
	list, err := mgr.ListConnections(ctx, schema.ConnectionListRequest{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(list.Body), 2)

	// A connection's query is cancelled, and it's returned, by its pid, as
	// is one which is ended, which is then gone. The one which ends it, or
	// cancels it, can't be, so either is tried, until one is another.
	var target *schema.Connection
	for i := range list.Body {
		conn, err := mgr.CancelConnection(ctx, uint64(list.Body[i].Pid))
		if err == nil {
			target = conn
			break
		}
	}
	require.NotNil(t, target)

	ended, err := mgr.DeleteConnection(ctx, uint64(target.Pid))
	require.NoError(t, err)
	assert.Equal(t, target.Pid, ended.Pid)

	// A pid which isn't a connection isn't found, rather than ending another
	_, err = mgr.DeleteConnection(ctx, 1)
	assert.ErrorIs(t, err, pg.ErrNotFound)
}
