package manager_test

import (
	"context"
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
