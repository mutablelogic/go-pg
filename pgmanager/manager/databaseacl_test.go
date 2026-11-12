package manager_test

import (
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	harness "github.com/mutablelogic/go-pg/pgmanager/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestDatabaseACLDefaults(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	// A new database has PostgreSQL's defaults, which are its access, so
	// everyone can connect, and use temporary tables
	db, err := mgr.CreateDatabase(ctx, schema.DatabaseMeta{Name: "acl_db"})
	require.NoError(t, err)
	defer func() { _, _ = mgr.DeleteDatabase(ctx, "acl_db", true) }()
	public := db.Acl.Find(schema.DefaultAclRole)
	if assert.NotNil(t, public) {
		assert.ElementsMatch(t, []string{"CONNECT", "TEMPORARY"}, public.Priv)
	}

	// The defaults can be revoked, by leaving everyone out
	owner := db.Acl.Find(db.Owner)
	require.NotNil(t, owner)
	db, err = mgr.UpdateDatabase(ctx, "acl_db", schema.DatabaseMeta{Acl: schema.ACLList{owner}})
	require.NoError(t, err)
	assert.Nil(t, db.Acl.Find(schema.DefaultAclRole))

	// An empty list revokes everything
	db, err = mgr.UpdateDatabase(ctx, "acl_db", schema.DatabaseMeta{Acl: schema.ACLList{}})
	require.NoError(t, err)
	assert.Empty(t, db.Acl)
}
