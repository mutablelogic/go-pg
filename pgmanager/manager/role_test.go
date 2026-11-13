package manager_test

import (
	"testing"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	harness "github.com/mutablelogic/go-pg/pgmanager/test"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestRoleLimitsAndMemberships(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	// A role is created with a connection limit, an expiry, and in a role
	expires := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	role, err := mgr.CreateRole(ctx, schema.RoleMeta{
		Name:            "limited_role",
		ConnectionLimit: types.Ptr(int64(5)),
		Expires:         types.Ptr(expires),
		Groups:          []string{"pg_monitor"},
	})
	require.NoError(t, err)
	defer func() { _, _ = mgr.DeleteRole(ctx, "limited_role") }()
	assert.Equal(t, int64(5), types.Value(role.ConnectionLimit))
	assert.True(t, expires.Equal(types.Value(role.Expires)))
	assert.Equal(t, []string{"pg_monitor"}, role.Groups)

	// No limit is -1, and no expiry is the zero time, which remove them
	role, err = mgr.UpdateRole(ctx, "limited_role", schema.RoleMeta{
		ConnectionLimit: types.Ptr(int64(-1)),
		Expires:         types.Ptr(time.Time{}),
	})
	require.NoError(t, err)
	assert.Nil(t, role.ConnectionLimit)
	assert.Nil(t, role.Expires)

	// An update without them leaves them
	role, err = mgr.UpdateRole(ctx, "limited_role", schema.RoleMeta{ConnectionLimit: types.Ptr(int64(2))})
	require.NoError(t, err)
	role, err = mgr.UpdateRole(ctx, "limited_role", schema.RoleMeta{Comment: types.Ptr("Limited")})
	require.NoError(t, err)
	assert.Equal(t, int64(2), types.Value(role.ConnectionLimit))
	assert.Nil(t, role.Expires)
}

func TestRoleInRoleQuoted(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	// A role created in a role is in it by its name, quoted, so a name
	// isn't SQL
	_, err := mgr.CreateRole(ctx, schema.RoleMeta{Name: "quoted_role", Groups: []string{"pg_monitor; DROP ROLE postgres"}})
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), `role "pg_monitor; DROP ROLE postgres" does not exist`)
	}
	_, _ = mgr.DeleteRole(ctx, "quoted_role")
}

func TestPredefinedRoleDescriptions(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	// The bootstrap describes the predefined roles which have no description
	role, err := mgr.GetRole(ctx, "pg_monitor")
	require.NoError(t, err)
	assert.True(t, role.Predefined)
	assert.Contains(t, types.Value(role.Comment), "Monitors the server")

	// Every one of them which this version of PostgreSQL has is described
	list, err := mgr.ListRoles(ctx, schema.RoleListRequest{Predefined: types.Ptr(true)})
	require.NoError(t, err)
	for _, role := range list.Body {
		assert.NotEmpty(t, types.Value(role.Comment), role.Name)
	}

	// The superuser which created the server is the administrator
	role, err = mgr.GetRole(ctx, "postgres")
	require.NoError(t, err)
	assert.Equal(t, "Administrator", types.Value(role.Comment))
}
