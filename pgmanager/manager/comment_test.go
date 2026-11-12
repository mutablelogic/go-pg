package manager_test

import (
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	harness "github.com/mutablelogic/go-pg/pgmanager/test"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// The resources are deleted with defer, which runs before harness.End,
// which cancels ctx, so they're gone before the next test
func TestCommentRole(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	// A role is created with a comment, which is changed, with a quote, and
	// removed, without changing anything else
	role, err := mgr.CreateRole(ctx, schema.RoleMeta{Name: "comment_role", Comment: types.Ptr("Reads the reports")})
	require.NoError(t, err)
	defer func() { _, _ = mgr.DeleteRole(ctx, "comment_role") }()
	assert.Equal(t, "Reads the reports", types.Value(role.Comment))

	role, err = mgr.UpdateRole(ctx, "comment_role", schema.RoleMeta{Comment: types.Ptr("Bob's reports")})
	require.NoError(t, err)
	assert.Equal(t, "Bob's reports", types.Value(role.Comment))

	role, err = mgr.UpdateRole(ctx, "comment_role", schema.RoleMeta{Comment: types.Ptr("")})
	require.NoError(t, err)
	assert.Nil(t, role.Comment)
}

func TestCommentDatabase(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	// A database is created with a comment, which is changed, and an update
	// without one leaves it
	db, err := mgr.CreateDatabase(ctx, schema.DatabaseMeta{Name: "comment_db", Comment: types.Ptr("The shop")})
	require.NoError(t, err)
	defer func() { _, _ = mgr.DeleteDatabase(ctx, "comment_db", true) }()
	assert.Equal(t, "The shop", types.Value(db.Comment))

	db, err = mgr.UpdateDatabase(ctx, "comment_db", schema.DatabaseMeta{Comment: types.Ptr("The new shop")})
	require.NoError(t, err)
	assert.Equal(t, "The new shop", types.Value(db.Comment))

	db, err = mgr.UpdateDatabase(ctx, "comment_db", schema.DatabaseMeta{})
	require.NoError(t, err)
	assert.Equal(t, "The new shop", types.Value(db.Comment))

	// A schema in it is created with a comment, which is changed
	s, err := mgr.CreateSchema(ctx, "comment_db", schema.SchemaMeta{Name: "sales", Comment: types.Ptr("Orders")})
	require.NoError(t, err)
	assert.Equal(t, "Orders", types.Value(s.Comment))

	s, err = mgr.UpdateSchema(ctx, "comment_db", "sales", schema.SchemaMeta{Comment: types.Ptr("Orders, and refunds")})
	require.NoError(t, err)
	assert.Equal(t, "Orders, and refunds", types.Value(s.Comment))
}
