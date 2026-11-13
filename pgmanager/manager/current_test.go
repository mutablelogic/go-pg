package manager_test

import (
	"context"
	"fmt"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pgmanager/manager"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	harness "github.com/mutablelogic/go-pg/pgmanager/test"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// pinned is one connection, as a pool, so what's current is known: every
// query, and transaction, is on it
type pinned struct {
	pg.PoolConn
	conn pg.Conn
}

func (p pinned) With(params ...any) pg.Conn { return pinned{p.PoolConn, p.conn.With(params...)} }
func (p pinned) Exec(ctx context.Context, q string) error { return p.conn.Exec(ctx, q) }
func (p pinned) Get(ctx context.Context, r pg.Reader, s pg.Selector) error {
	return p.conn.Get(ctx, r, s)
}
func (p pinned) List(ctx context.Context, r pg.Reader, s pg.Selector) error {
	return p.conn.List(ctx, r, s)
}
func (p pinned) Delete(ctx context.Context, r pg.Reader, s pg.Selector) error {
	return p.conn.Delete(ctx, r, s)
}
func (p pinned) Tx(_ context.Context, fn func(pg.Conn) error) error { return fn(p) }

// The database pgmanager connects to, and the role it runs as, are current,
// and can't be deleted
func TestCurrentDatabaseAndRole(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	databases, err := mgr.ListDatabases(ctx, schema.DatabaseListRequest{})
	require.NoError(t, err)
	var current []string
	for _, db := range databases.Body {
		if db.Current {
			current = append(current, db.Name)
		}
	}
	require.Len(t, current, 1)
	_, err = mgr.DeleteDatabase(ctx, current[0], true)
	assert.ErrorIs(t, err, pg.ErrConflict)

	// Nor renamed, though its description can change
	_, err = mgr.UpdateDatabase(ctx, current[0], schema.DatabaseMeta{Name: current[0] + "_renamed"})
	assert.ErrorIs(t, err, pg.ErrConflict)
	db, err := mgr.UpdateDatabase(ctx, current[0], schema.DatabaseMeta{Comment: types.Ptr("Renamed? No")})
	require.NoError(t, err)
	assert.Equal(t, current[0], db.Name)
	assert.Equal(t, "Renamed? No", types.Value(db.Comment))
	_, _ = mgr.UpdateDatabase(ctx, current[0], schema.DatabaseMeta{Comment: types.Ptr("")})

	roles, err := mgr.ListRoles(ctx, schema.RoleListRequest{})
	require.NoError(t, err)
	current = nil
	for _, role := range roles.Body {
		if role.Current {
			current = append(current, role.Name)
		}
	}
	require.Equal(t, []string{"postgres"}, current)
	_, err = mgr.DeleteRole(ctx, "postgres")
	assert.ErrorIs(t, err, pg.ErrConflict)

	// Another role isn't current, and is deleted
	_, err = mgr.CreateRole(ctx, schema.RoleMeta{Name: "not_current"})
	require.NoError(t, err)
	role, err := mgr.GetRole(ctx, "not_current")
	require.NoError(t, err)
	assert.False(t, role.Current)
	_, err = mgr.DeleteRole(ctx, "not_current")
	assert.NoError(t, err)
}

// The connection which lists the connections is current, and can't be
// ended, or cancelled
func TestCurrentConnection(t *testing.T) {
	_, ctx := harness.Begin(t)
	defer harness.End(t)

	require.NoError(t, harness.Pool().Tx(ctx, func(conn pg.Conn) error {
		mgr, err := manager.New(pinned{harness.Pool(), conn})
		require.NoError(t, err)

		list, err := mgr.ListConnections(ctx, schema.ConnectionListRequest{Limit: types.Ptr(uint64(100))})
		require.NoError(t, err)
		var current []uint64
		for _, c := range list.Body {
			if c.Current {
				current = append(current, uint64(c.Pid))
			}
		}
		require.Len(t, current, 1)

		_, err = mgr.DeleteConnection(ctx, current[0])
		assert.ErrorIs(t, err, pg.ErrConflict)
		_, err = mgr.CancelConnection(ctx, current[0])
		assert.ErrorIs(t, err, pg.ErrConflict)
		return nil
	}))
}

// recorder is the pool, which records each query's selector, as pool: or
// tx:, as it's in a transaction or not, so what's on one session is known
type recorder struct {
	pg.PoolConn
	tx  bool
	ops *[]string
}

func (r recorder) record(sel pg.Selector) {
	where := "pool"
	if r.tx {
		where = "tx"
	}
	*r.ops = append(*r.ops, fmt.Sprintf("%s:%T", where, sel))
}
func (r recorder) With(params ...any) pg.Conn {
	return recorder{r.PoolConn.With(params...).(pg.PoolConn), r.tx, r.ops}
}
func (r recorder) Get(ctx context.Context, rd pg.Reader, s pg.Selector) error {
	r.record(s)
	return r.PoolConn.Get(ctx, rd, s)
}
func (r recorder) Delete(ctx context.Context, rd pg.Reader, s pg.Selector) error {
	r.record(s)
	return r.PoolConn.Delete(ctx, rd, s)
}
func (r recorder) Tx(ctx context.Context, fn func(pg.Conn) error) error {
	return r.PoolConn.Tx(ctx, func(conn pg.Conn) error {
		return fn(txRecorder{conn, r.ops})
	})
}

// txRecorder is a transaction's connection, whose queries are recorded as
// tx:
type txRecorder struct {
	pg.Conn
	ops *[]string
}

func (r txRecorder) With(params ...any) pg.Conn { return txRecorder{r.Conn.With(params...), r.ops} }
func (r txRecorder) Get(ctx context.Context, rd pg.Reader, s pg.Selector) error {
	*r.ops = append(*r.ops, fmt.Sprintf("tx:%T", s))
	return r.Conn.Get(ctx, rd, s)
}
func (r txRecorder) Delete(ctx context.Context, rd pg.Reader, s pg.Selector) error {
	*r.ops = append(*r.ops, fmt.Sprintf("tx:%T", s))
	return r.Conn.Delete(ctx, rd, s)
}

// Ending, or cancelling, a connection checks it isn't the current one on
// the session which ends, or cancels it, in a transaction, and nothing of
// it is on the pool, whose sessions are others
func TestCurrentConnectionSameSession(t *testing.T) {
	shared, ctx := harness.Begin(t)
	defer harness.End(t)

	var ops []string
	mgr, err := manager.New(recorder{harness.Pool(), false, &ops})
	require.NoError(t, err)

	// Cancel a sleeping connection's query
	pid, done := sleeping(t, ctx, shared, "same-session-cancel")
	ops = nil
	_, err = mgr.CancelConnection(ctx, pid)
	require.NoError(t, err)
	<-done
	assert.Equal(t, []string{"tx:schema.ConnectionPid", "tx:schema.ConnectionCancel"}, ops)

	// End another
	pid, done = sleeping(t, ctx, shared, "same-session-end")
	ops = nil
	_, err = mgr.DeleteConnection(ctx, pid)
	require.NoError(t, err)
	<-done
	assert.Equal(t, []string{"tx:schema.ConnectionPid", "tx:schema.ConnectionPid"}, ops)
}
