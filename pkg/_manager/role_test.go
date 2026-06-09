package manager_test

import (
	"context"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pkg/manager"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
)

////////////////////////////////////////////////////////////////////////////////
// LIST ROLES TESTS

func Test_Manager_ListRoles(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("ListAll", func(t *testing.T) {
		roles, err := mgr.ListRoles(context.TODO(), schema.RoleListRequest{})
		assert.NoError(err)
		assert.NotNil(roles)
		assert.Equal(len(roles.Body), int(roles.Count))
		// Should have at least one role (the superuser)
		assert.GreaterOrEqual(roles.Count, uint64(1))
	})

	t.Run("ListWithPagination", func(t *testing.T) {
		limit := uint64(1)
		roles, err := mgr.ListRoles(context.TODO(), schema.RoleListRequest{
			OffsetLimit: pg.OffsetLimit{Limit: &limit},
		})
		assert.NoError(err)
		assert.NotNil(roles)
		assert.LessOrEqual(len(roles.Body), 1)
	})
}

////////////////////////////////////////////////////////////////////////////////
// GET ROLE TESTS

func Test_Manager_GetRole(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("GetExisting", func(t *testing.T) {
		// First create a role to get
		roleName := "test_get_existing"
		t.Cleanup(func() {
			mgr.DeleteRole(context.TODO(), roleName)
		})

		_, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name: roleName,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		role, err := mgr.GetRole(context.TODO(), roleName)
		assert.NoError(err)
		assert.NotNil(role)
		assert.Equal(roleName, role.Name)
	})

	t.Run("GetNonExistent", func(t *testing.T) {
		_, err := mgr.GetRole(context.TODO(), "non_existing_role_xyz")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("GetEmptyName", func(t *testing.T) {
		_, err := mgr.GetRole(context.TODO(), "")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})
}

////////////////////////////////////////////////////////////////////////////////
// CREATE ROLE TESTS

func Test_Manager_CreateRole(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("CreateSimple", func(t *testing.T) {
		roleName := "test_create_simple_role"
		t.Cleanup(func() {
			mgr.DeleteRole(context.TODO(), roleName)
		})

		role, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name: roleName,
		})
		assert.NoError(err)
		assert.NotNil(role)
		assert.Equal(roleName, role.Name)
	})

	t.Run("CreateWithLogin", func(t *testing.T) {
		roleName := "test_create_login_role"
		t.Cleanup(func() {
			mgr.DeleteRole(context.TODO(), roleName)
		})

		role, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name:  roleName,
			Login: types.BoolPtr(true),
		})
		assert.NoError(err)
		assert.NotNil(role)
		assert.Equal(roleName, role.Name)
		assert.True(types.PtrBool(role.Login))
	})

	t.Run("CreateWithPassword", func(t *testing.T) {
		roleName := "test_create_password_role"
		t.Cleanup(func() {
			mgr.DeleteRole(context.TODO(), roleName)
		})

		role, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name:     roleName,
			Login:    types.BoolPtr(true),
			Password: types.StringPtr("secret123"),
		})
		assert.NoError(err)
		assert.NotNil(role)
		assert.Equal(roleName, role.Name)
		// Password should be obfuscated in response
		assert.NotNil(role.Password)
	})

	t.Run("CreateWithConnectionLimit", func(t *testing.T) {
		roleName := "test_create_connlimit_role"
		t.Cleanup(func() {
			mgr.DeleteRole(context.TODO(), roleName)
		})

		role, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name:            roleName,
			ConnectionLimit: types.Uint64Ptr(5),
		})
		assert.NoError(err)
		assert.NotNil(role)
		assert.Equal(uint64(5), types.PtrUint64(role.ConnectionLimit))
	})

	t.Run("CreateWithPermissions", func(t *testing.T) {
		roleName := "test_create_perms_role"
		t.Cleanup(func() {
			mgr.DeleteRole(context.TODO(), roleName)
		})

		role, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name:            roleName,
			CreateDatabases: types.BoolPtr(true),
			CreateRoles:     types.BoolPtr(true),
		})
		assert.NoError(err)
		assert.NotNil(role)
		assert.True(types.PtrBool(role.CreateDatabases))
		assert.True(types.PtrBool(role.CreateRoles))
	})

	t.Run("CreateEmptyName", func(t *testing.T) {
		_, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name: "",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("CreateReservedPrefix", func(t *testing.T) {
		_, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name: "pg_reserved_role",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		roleName := "test_create_duplicate_role"
		t.Cleanup(func() {
			mgr.DeleteRole(context.TODO(), roleName)
		})

		_, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name: roleName,
		})
		assert.NoError(err)

		// Try to create again
		_, err = mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name: roleName,
		})
		assert.Error(err)
	})

	t.Run("CreateWithGroups", func(t *testing.T) {
		groupName := "test_group_parent"
		roleName := "test_create_with_groups"
		t.Cleanup(func() {
			mgr.DeleteRole(context.TODO(), roleName)
			mgr.DeleteRole(context.TODO(), groupName)
		})

		// Create the group first
		_, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name: groupName,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Create role as member of the group
		role, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name:   roleName,
			Groups: []string{groupName},
		})
		assert.NoError(err)
		assert.NotNil(role)
		assert.Contains(role.Groups, groupName)
	})
}

////////////////////////////////////////////////////////////////////////////////
// DELETE ROLE TESTS

func Test_Manager_DeleteRole(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("DeleteExisting", func(t *testing.T) {
		roleName := "test_delete_existing_role"

		// Create first
		_, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name: roleName,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Delete
		role, err := mgr.DeleteRole(context.TODO(), roleName)
		assert.NoError(err)
		assert.NotNil(role)
		assert.Equal(roleName, role.Name)

		// Verify it's gone
		_, err = mgr.GetRole(context.TODO(), roleName)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {
		_, err := mgr.DeleteRole(context.TODO(), "non_existing_role_xyz")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("DeleteEmptyName", func(t *testing.T) {
		_, err := mgr.DeleteRole(context.TODO(), "")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})
}

////////////////////////////////////////////////////////////////////////////////
// UPDATE ROLE TESTS

func Test_Manager_UpdateRole(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("RenameRole", func(t *testing.T) {
		oldName := "test_rename_old_role"
		newName := "test_rename_new_role"
		t.Cleanup(func() {
			mgr.DeleteRole(context.TODO(), oldName)
			mgr.DeleteRole(context.TODO(), newName)
		})

		// Create
		_, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name: oldName,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Rename
		role, err := mgr.UpdateRole(context.TODO(), oldName, schema.RoleMeta{
			Name: newName,
		})
		assert.NoError(err)
		assert.NotNil(role)
		assert.Equal(newName, role.Name)

		// Old name should not exist
		_, err = mgr.GetRole(context.TODO(), oldName)
		assert.ErrorIs(err, pg.ErrNotFound)

		// New name should exist
		_, err = mgr.GetRole(context.TODO(), newName)
		assert.NoError(err)
	})

	t.Run("UpdateLogin", func(t *testing.T) {
		roleName := "test_update_login_role"
		t.Cleanup(func() {
			mgr.DeleteRole(context.TODO(), roleName)
		})

		// Create without login
		_, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name:  roleName,
			Login: types.BoolPtr(false),
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Enable login
		role, err := mgr.UpdateRole(context.TODO(), roleName, schema.RoleMeta{
			Login: types.BoolPtr(true),
		})
		assert.NoError(err)
		assert.NotNil(role)
		assert.True(types.PtrBool(role.Login))
	})

	t.Run("UpdateConnectionLimit", func(t *testing.T) {
		roleName := "test_update_connlimit_role"
		t.Cleanup(func() {
			mgr.DeleteRole(context.TODO(), roleName)
		})

		// Create
		_, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name: roleName,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Update connection limit
		role, err := mgr.UpdateRole(context.TODO(), roleName, schema.RoleMeta{
			ConnectionLimit: types.Uint64Ptr(10),
		})
		assert.NoError(err)
		assert.NotNil(role)
		assert.Equal(uint64(10), types.PtrUint64(role.ConnectionLimit))
	})

	t.Run("UpdateEmptyName", func(t *testing.T) {
		_, err := mgr.UpdateRole(context.TODO(), "", schema.RoleMeta{
			Login: types.BoolPtr(true),
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("UpdateToReservedPrefix", func(t *testing.T) {
		roleName := "test_update_reserved_role"
		t.Cleanup(func() {
			mgr.DeleteRole(context.TODO(), roleName)
		})

		// Create
		_, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name: roleName,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Try to rename to reserved prefix
		_, err = mgr.UpdateRole(context.TODO(), roleName, schema.RoleMeta{
			Name: "pg_reserved",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("UpdateNonExistent", func(t *testing.T) {
		_, err := mgr.UpdateRole(context.TODO(), "non_existing_role_xyz", schema.RoleMeta{
			Login: types.BoolPtr(true),
		})
		assert.Error(err)
	})

	t.Run("UpdateAddGroupMembership", func(t *testing.T) {
		groupName := "test_update_group"
		roleName := "test_update_add_member"
		t.Cleanup(func() {
			mgr.DeleteRole(context.TODO(), roleName)
			mgr.DeleteRole(context.TODO(), groupName)
		})

		// Create the group
		_, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name: groupName,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Create the role without membership
		_, err = mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name: roleName,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Add to group
		role, err := mgr.UpdateRole(context.TODO(), roleName, schema.RoleMeta{
			Groups: []string{groupName},
		})
		assert.NoError(err)
		assert.NotNil(role)
		assert.Contains(role.Groups, groupName)
	})

	t.Run("UpdateRemoveGroupMembership", func(t *testing.T) {
		groupName := "test_update_remove_group"
		roleName := "test_update_remove_member"
		t.Cleanup(func() {
			mgr.DeleteRole(context.TODO(), roleName)
			mgr.DeleteRole(context.TODO(), groupName)
		})

		// Create the group
		_, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name: groupName,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Create the role with membership
		_, err = mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name:   roleName,
			Groups: []string{groupName},
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Remove from group by setting empty groups
		role, err := mgr.UpdateRole(context.TODO(), roleName, schema.RoleMeta{
			Groups: []string{},
		})
		assert.NoError(err)
		assert.NotNil(role)
		assert.NotContains(role.Groups, groupName)
	})

	t.Run("UpdateMultipleGroupMemberships", func(t *testing.T) {
		group1Name := "test_multi_group1"
		group2Name := "test_multi_group2"
		group3Name := "test_multi_group3"
		roleName := "test_multi_member"
		t.Cleanup(func() {
			mgr.DeleteRole(context.TODO(), roleName)
			mgr.DeleteRole(context.TODO(), group1Name)
			mgr.DeleteRole(context.TODO(), group2Name)
			mgr.DeleteRole(context.TODO(), group3Name)
		})

		// Create the groups
		for _, gname := range []string{group1Name, group2Name, group3Name} {
			_, err := mgr.CreateRole(context.TODO(), schema.RoleMeta{
				Name: gname,
			})
			if !assert.NoError(err) {
				t.FailNow()
			}
		}

		// Create the role with membership in group1 and group2
		_, err = mgr.CreateRole(context.TODO(), schema.RoleMeta{
			Name:   roleName,
			Groups: []string{group1Name, group2Name},
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Update to be member of group2 and group3 (remove group1, keep group2, add group3)
		role, err := mgr.UpdateRole(context.TODO(), roleName, schema.RoleMeta{
			Groups: []string{group2Name, group3Name},
		})
		assert.NoError(err)
		assert.NotNil(role)
		assert.NotContains(role.Groups, group1Name)
		assert.Contains(role.Groups, group2Name)
		assert.Contains(role.Groups, group3Name)
	})
}
