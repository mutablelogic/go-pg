package pg

import (
	"testing"

	// Packages
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Opts_001(t *testing.T) {
	assert := assert.New(t)

	// Empty options
	o, err := apply()
	if assert.NoError(err) {
		assert.NotNil(o)
	}
}

func Test_Opts_002(t *testing.T) {
	assert := assert.New(t)

	// Empty options
	o, err := apply()
	if assert.NoError(err) {
		assert.NotNil(o)
		assert.Equal("host=localhost pool_max_conns=10 port=5432", o.Encode())
	}
}

func Test_Opts_003(t *testing.T) {
	assert := assert.New(t)

	// Host/Port
	o, err := apply(
		WithHostPort("host", "999"),
	)
	if assert.NoError(err) {
		assert.NotNil(o)
		assert.Equal("host=host pool_max_conns=10 port=999", o.Encode())
	}
}

func Test_Opts_004(t *testing.T) {
	assert := assert.New(t)

	// Host/Port
	o, err := apply(
		WithCredentials("user", "password"),
	)
	if assert.NoError(err) {
		assert.NotNil(o)
		assert.Equal("dbname=user host=localhost password=password pool_max_conns=10 port=5432 user=user", o.Encode())
	}
}

func Test_Opts_005(t *testing.T) {
	assert := assert.New(t)

	// Host/Port
	o, err := apply(
		WithCredentials("user", "password"),
		WithDatabase("db"),
		WithHostPort("host", "999"),
	)
	if assert.NoError(err) {
		assert.NotNil(o)
		assert.Equal("dbname=db host=host password=password pool_max_conns=10 port=999 user=user", o.Encode())
	}
}

func Test_Opts_006(t *testing.T) {
	assert := assert.New(t)

	// Host/Port
	o, err := apply(
		WithSSLMode("disable"),
	)
	if assert.NoError(err) {
		assert.NotNil(o)
		assert.Equal("host=localhost pool_max_conns=10 port=5432 sslmode=disable", o.Encode())
	}
}

func Test_Opts_007(t *testing.T) {
	assert := assert.New(t)

	// WithPassword sets password without affecting other fields
	o, err := apply(
		WithPassword("secret"),
	)
	if assert.NoError(err) {
		assert.Equal("host=localhost password=secret pool_max_conns=10 port=5432", o.Encode())
	}
}

func Test_Opts_008(t *testing.T) {
	assert := assert.New(t)

	// WithPassword overrides a password already set by WithCredentials
	o, err := apply(
		WithCredentials("user", "original"),
		WithPassword("override"),
	)
	if assert.NoError(err) {
		// username and dbname from WithCredentials must be preserved
		assert.Equal("dbname=user host=localhost password=override pool_max_conns=10 port=5432 user=user", o.Encode())
	}
}

func Test_Opts_009(t *testing.T) {
	assert := assert.New(t)

	// WithPassword with empty string leaves existing password unchanged
	o, err := apply(
		WithCredentials("user", "original"),
		WithPassword(""),
	)
	if assert.NoError(err) {
		assert.Equal("dbname=user host=localhost password=original pool_max_conns=10 port=5432 user=user", o.Encode())
	}
}

func Test_Opts_010(t *testing.T) {
	assert := assert.New(t)

	// An application name, which the URL's replaces, when it has one
	o, err := apply(WithApplicationName("app"), WithURL("postgres://localhost/db"))
	if assert.NoError(err) {
		assert.Equal("app", o.Get("application_name"))
	}
	o, err = apply(WithApplicationName("app"), WithURL("postgres://localhost/db?application_name=other"))
	if assert.NoError(err) {
		assert.Equal([]string{"other"}, o.Values["application_name"])
	}
}

// Values with spaces, quotes or backslashes are quoted, and parse as they
// were, and others are as they were
func Test_Opts_Quote(t *testing.T) {
	o, err := apply(WithCredentials(`Ops Team`, `pass word`), WithDatabase(`O'Brien\db`))
	require.NoError(t, err)
	assert.Equal(t, `dbname='O\'Brien\\db' host=localhost password='pass word' pool_max_conns=10 port=5432 user='Ops Team'`, o.Encode())

	config, err := pgxpool.ParseConfig(o.Encode())
	require.NoError(t, err)
	assert.Equal(t, "Ops Team", config.ConnConfig.User)
	assert.Equal(t, "pass word", config.ConnConfig.Password)
	assert.Equal(t, `O'Brien\db`, config.ConnConfig.Database)
}
