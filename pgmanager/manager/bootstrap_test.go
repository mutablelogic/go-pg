package manager_test

import (
	"context"
	"strings"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	pgtest "github.com/mutablelogic/go-pg/pkg/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// description is a role's description, as a reader
type description struct{ value *string }

func (d *description) Scan(row pg.Row) error { return row.Scan(&d.value) }

// statement is a query, as a selector
type statement string

func (s statement) Select(*pg.Bind, pg.Op) (string, error) { return string(s), nil }

// The superuser which created the server is found by its OID, whatever its
// name, even one which needs quoting, and is the administrator, unless it
// has a description, which a bootstrap keeps. (PostgreSQL won't initialise
// a server whose superuser's name has a quote, so it has a space, and
// capitals, which need quoting too.)
func TestBootstrapAdministrator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// A server whose superuser isn't postgres
	const superuser = "Ops Team"
	container, err := pgtest.NewContainer(ctx, "bootstrap-administrator", "ghcr.io/mutablelogic/docker-postgres:17-bookworm",
		pgtest.OptPostgres(superuser, "password", "bootstrap"),
	)
	require.NoError(t, err)
	defer func() { _ = container.Close(context.Background()) }()
	host, _ := container.GetEnv("POSTGRES_HOST")
	port, err := container.GetPort("5432/tcp")
	require.NoError(t, err)
	pool, err := pg.NewPool(ctx,
		pg.WithCredentials(superuser, "password"),
		pg.WithDatabase("bootstrap"),
		pg.WithHostPort(host, port),
	)
	require.NoError(t, err)
	defer pool.Close()

	comment := func() *string {
		t.Helper()
		var d description
		require.NoError(t, pool.Get(ctx, &d, statement(`SELECT shobj_description(10, 'pg_authid')`)))
		return d.value
	}

	// Without a description, it's the administrator
	require.Nil(t, comment())
	_, err = schema.Bootstrap(ctx, pool)
	require.NoError(t, err)
	assert.Equal(t, "Administrator", *comment())

	// With one of its own, it's kept, when the bootstrap runs again
	require.NoError(t, pool.Exec(ctx, `COMMENT ON ROLE "Ops Team" IS 'Runs everything'`))
	_, err = schema.Bootstrap(ctx, pool)
	require.NoError(t, err)
	assert.Equal(t, "Runs everything", *comment())
	assert.False(t, strings.Contains(*comment(), "Administrator"))
}
