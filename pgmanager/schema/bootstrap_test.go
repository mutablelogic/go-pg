package schema_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// fakeConn is a server, which is a replica, or not, with dblink, or not,
// and whose pg_stat_statements can be read, or not, which records what's
// executed. What isn't Exec, or Get, isn't expected.
type fakeConn struct {
	pg.PoolConn
	replica, dblink, statements bool
	executed                    []string
}

func (c *fakeConn) Exec(_ context.Context, query string) error {
	c.executed = append(c.executed, query)
	if strings.Contains(query, "pg_stat_statements") && !strings.HasPrefix(query, "CREATE") && !c.statements {
		return errors.New(`pg_stat_statements must be loaded via "shared_preload_libraries"`)
	}
	return nil
}

func (c *fakeConn) Get(_ context.Context, reader pg.Reader, sel pg.Selector) error {
	query, err := sel.Select(pg.NewBind(), pg.Get)
	if err != nil {
		return err
	}
	switch {
	case strings.Contains(query, "pg_is_in_recovery"):
		return reader.Scan(boolRow(c.replica))
	case strings.Contains(query, "dblink"):
		return reader.Scan(boolRow(c.dblink))
	default:
		return errors.New("unexpected query: " + query)
	}
}

// boolRow is a row of a bool
type boolRow bool

func (r boolRow) Scan(dest ...any) error {
	*(dest[0].(*bool)) = bool(r)
	return nil
}

// creates are the statements which would write, of those executed
func creates(executed []string) []string {
	var writes []string
	for _, q := range executed {
		if strings.HasPrefix(strings.TrimSpace(q), "CREATE") || strings.Contains(q, "COMMENT ON") {
			writes = append(writes, q)
		}
	}
	return writes
}

func Test_Bootstrap_Replica(t *testing.T) {
	// A replica, with dblink, and pg_stat_statements, creates nothing
	conn := &fakeConn{replica: true, dblink: true, statements: true}
	result, err := schema.Bootstrap(context.Background(), conn)
	require.NoError(t, err)
	assert.True(t, result.Replica)
	assert.True(t, result.StatStatementsAvailable)
	assert.Empty(t, creates(conn.executed))

	// One whose pg_stat_statements can't be read starts, without them
	conn = &fakeConn{replica: true, dblink: true}
	result, err = schema.Bootstrap(context.Background(), conn)
	require.NoError(t, err)
	assert.True(t, result.Replica)
	assert.False(t, result.StatStatementsAvailable)
	assert.Empty(t, creates(conn.executed))

	// One without dblink, which pgmanager needs, isn't available
	conn = &fakeConn{replica: true}
	_, err = schema.Bootstrap(context.Background(), conn)
	assert.ErrorIs(t, err, pg.ErrNotAvailable)
	assert.Empty(t, conn.executed)
}

func Test_Bootstrap_Primary(t *testing.T) {
	// A primary creates dblink, and pg_stat_statements, and describes the
	// predefined roles
	conn := &fakeConn{statements: true}
	result, err := schema.Bootstrap(context.Background(), conn)
	require.NoError(t, err)
	assert.False(t, result.Replica)
	assert.True(t, result.StatStatementsAvailable)
	writes := creates(conn.executed)
	assert.True(t, strings.Contains(strings.Join(writes, "\n"), "CREATE EXTENSION IF NOT EXISTS dblink"))
	assert.True(t, strings.Contains(strings.Join(writes, "\n"), "COMMENT ON ROLE"))
}
