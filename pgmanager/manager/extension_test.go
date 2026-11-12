package manager_test

import (
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	harness "github.com/mutablelogic/go-pg/pgmanager/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestExtensionInstallAndRemove(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	// An extension which is only available isn't installed, and can be
	_, err := mgr.GetInstalledExtension(ctx, "hstore", "postgres")
	assert.ErrorIs(t, err, pg.ErrNotFound)
	ext, err := mgr.CreateExtension(ctx, schema.ExtensionMeta{Name: "hstore", Database: "postgres"}, false)
	require.NoError(t, err)
	defer func() { _ = mgr.DeleteExtension(ctx, "postgres", "hstore", true) }()
	assert.NotNil(t, ext.InstalledVersion)

	// Once it's installed, it's found, and isn't installed again
	_, err = mgr.GetInstalledExtension(ctx, "hstore", "postgres")
	assert.NoError(t, err)
	_, err = mgr.CreateExtension(ctx, schema.ExtensionMeta{Name: "hstore", Database: "postgres"}, false)
	assert.ErrorIs(t, err, pg.ErrConflict)

	// One which requires another is installed with it, with cascade
	_, err = mgr.CreateExtension(ctx, schema.ExtensionMeta{Name: "earthdistance", Database: "postgres"}, true)
	require.NoError(t, err)
	defer func() { _ = mgr.DeleteExtension(ctx, "postgres", "cube", true) }()
	_, err = mgr.GetInstalledExtension(ctx, "cube", "postgres")
	assert.NoError(t, err)

	// One which another depends on isn't removed, unless with cascade
	assert.ErrorIs(t, mgr.DeleteExtension(ctx, "postgres", "cube", false), pg.ErrConflict)
	assert.NoError(t, mgr.DeleteExtension(ctx, "postgres", "cube", true))
	_, err = mgr.GetInstalledExtension(ctx, "earthdistance", "postgres")
	assert.ErrorIs(t, err, pg.ErrNotFound)
}
