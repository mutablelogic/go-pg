package manager_test

import (
	"testing"

	// Packages
	harness "github.com/mutablelogic/go-pg/pgmanager/test"
	require "github.com/stretchr/testify/require"
)

func TestStatusPing(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	require.NoError(t, mgr.Ping(ctx), "ping failed")
}
