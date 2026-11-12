package manager_test

import (
	"testing"

	// Packages
	harness "github.com/mutablelogic/go-pg/pgmanager/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestTablespaceDatabases(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	// pg_default has the files of the databases which default to it, such
	// as postgres
	space, err := mgr.GetTablespace(ctx, "pg_default")
	require.NoError(t, err)
	assert.Contains(t, space.Databases, "postgres")
}
