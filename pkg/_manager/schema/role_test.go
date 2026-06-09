package schema_test

import (
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
)

func Test_RoleMeta_Validate(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidName", func(t *testing.T) {
		r := schema.RoleMeta{Name: "testrole"}
		err := r.Validate()
		assert.NoError(err)
	})

	t.Run("ValidNameWithUnderscore", func(t *testing.T) {
		r := schema.RoleMeta{Name: "test_role"}
		err := r.Validate()
		assert.NoError(err)
	})

	t.Run("ValidNameWithNumbers", func(t *testing.T) {
		r := schema.RoleMeta{Name: "role123"}
		err := r.Validate()
		assert.NoError(err)
	})

	t.Run("EmptyName", func(t *testing.T) {
		r := schema.RoleMeta{Name: ""}
		err := r.Validate()
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("WhitespaceOnlyName", func(t *testing.T) {
		r := schema.RoleMeta{Name: "   "}
		err := r.Validate()
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("ReservedPrefixName", func(t *testing.T) {
		r := schema.RoleMeta{Name: "pg_admin"}
		err := r.Validate()
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("ReservedPrefixName2", func(t *testing.T) {
		r := schema.RoleMeta{Name: "pg_"}
		err := r.Validate()
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("NameWithSpaces", func(t *testing.T) {
		r := schema.RoleMeta{Name: "  testrole  "}
		err := r.Validate()
		assert.NoError(err) // Trimmed name is valid
	})

	t.Run("InvalidIdentifier", func(t *testing.T) {
		r := schema.RoleMeta{Name: "123role"}
		err := r.Validate()
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("InvalidIdentifierWithSpecialChar", func(t *testing.T) {
		r := schema.RoleMeta{Name: "role@name"}
		err := r.Validate()
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})
}

func Test_RoleName_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("GetOperation", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleName("testrole")
		sql, err := r.Select(bind, pg.Get)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("testrole", bind.Get("name"))
	})

	t.Run("DeleteOperation", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleName("testrole")
		sql, err := r.Select(bind, pg.Delete)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "DROP ROLE")
	})

	t.Run("UpdateOperation", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleName("oldrole")
		sql, err := r.Select(bind, pg.Update)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "RENAME")
	})

	t.Run("EmptyName", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleName("")
		_, err := r.Select(bind, pg.Get)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("ReservedPrefixForUpdate", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleName("pg_admin")
		_, err := r.Select(bind, pg.Update)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("ReservedPrefixForDelete", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleName("pg_admin")
		_, err := r.Select(bind, pg.Delete)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("ReservedPrefixAllowedForGet", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleName("pg_monitor")
		sql, err := r.Select(bind, pg.Get)
		assert.NoError(err)
		assert.NotEmpty(sql)
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleName("testrole")
		_, err := r.Select(bind, pg.Insert)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotImplemented)
	})
}

func Test_RoleName_Update(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidUpdate", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleName("oldrole")
		err := r.Update(bind)
		assert.NoError(err)
		assert.Equal("oldrole", bind.Get("old_name"))
	})

	t.Run("EmptyName", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleName("")
		err := r.Update(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("WhitespaceName", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleName("   ")
		err := r.Update(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("ReservedPrefixName", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleName("pg_admin")
		err := r.Update(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("InvalidIdentifier", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleName("123role")
		err := r.Update(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})
}

func Test_RoleName_Insert(t *testing.T) {
	assert := assert.New(t)

	t.Run("NotImplemented", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleName("testrole")
		_, err := r.Insert(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotImplemented)
	})
}

func Test_RoleMeta_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("UpdateOperation", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{Name: "testrole"}
		sql, err := r.Select(bind, pg.Update)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "ALTER ROLE")
		assert.Equal("testrole", bind.Get("name"))
	})

	t.Run("EmptyName", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{Name: ""}
		_, err := r.Select(bind, pg.Update)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("ReservedPrefixName", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{Name: "pg_admin"}
		_, err := r.Select(bind, pg.Update)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{Name: "testrole"}
		_, err := r.Select(bind, pg.Get)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotImplemented)
	})
}

func Test_RoleMeta_Insert(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidInsert", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{Name: "newrole"}
		sql, err := r.Insert(bind)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "CREATE ROLE")
		assert.Equal("newrole", bind.Get("name"))
	})

	t.Run("EmptyName", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{Name: ""}
		_, err := r.Insert(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("ReservedPrefixName", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{Name: "pg_admin"}
		_, err := r.Insert(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("InsertWithSuperuser", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{Name: "superuser_role", Superuser: types.BoolPtr(true)}
		sql, err := r.Insert(bind)
		assert.NoError(err)
		assert.NotEmpty(sql)
		with := bind.Get("with").(string)
		assert.Contains(with, "SUPERUSER")
	})

	t.Run("InsertWithNoLogin", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{Name: "group_role", Login: types.BoolPtr(false)}
		sql, err := r.Insert(bind)
		assert.NoError(err)
		assert.NotEmpty(sql)
		with := bind.Get("with").(string)
		assert.Contains(with, "NOLOGIN")
	})

	t.Run("InsertWithConnectionLimit", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{Name: "limited_role", ConnectionLimit: types.Uint64Ptr(5)}
		sql, err := r.Insert(bind)
		assert.NoError(err)
		assert.NotEmpty(sql)
		with := bind.Get("with").(string)
		assert.Contains(with, "CONNECTION LIMIT 5")
	})

	t.Run("InsertWithPassword", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{Name: "user_role", Password: types.StringPtr("secret123")}
		sql, err := r.Insert(bind)
		assert.NoError(err)
		assert.NotEmpty(sql)
		with := bind.Get("with").(string)
		assert.Contains(with, "PASSWORD")
	})

	t.Run("InsertWithNullPassword", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{Name: "nopass_role", Password: types.StringPtr("")}
		sql, err := r.Insert(bind)
		assert.NoError(err)
		assert.NotEmpty(sql)
		with := bind.Get("with").(string)
		assert.Contains(with, "PASSWORD NULL")
	})

	t.Run("InsertWithExpires", func(t *testing.T) {
		bind := pg.NewBind()
		expires := time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC)
		r := schema.RoleMeta{Name: "temp_role", Expires: &expires}
		sql, err := r.Insert(bind)
		assert.NoError(err)
		assert.NotEmpty(sql)
		with := bind.Get("with").(string)
		assert.Contains(with, "VALID UNTIL")
		assert.Contains(with, "2025-12-31")
	})

	t.Run("InsertWithGroups", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{Name: "member_role", Groups: []string{"group1", "group2"}}
		sql, err := r.Insert(bind)
		assert.NoError(err)
		assert.NotEmpty(sql)
		with := bind.Get("with").(string)
		assert.Contains(with, "IN ROLE")
		assert.Contains(with, "group1")
		assert.Contains(with, "group2")
	})

	t.Run("InsertWithMultipleOptions", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{
			Name:            "full_role",
			Superuser:       types.BoolPtr(false),
			Login:           types.BoolPtr(true),
			CreateDatabases: types.BoolPtr(true),
			CreateRoles:     types.BoolPtr(false),
			Inherit:         types.BoolPtr(true),
		}
		sql, err := r.Insert(bind)
		assert.NoError(err)
		assert.NotEmpty(sql)
		with := bind.Get("with").(string)
		assert.Contains(with, "NOSUPERUSER")
		assert.Contains(with, "LOGIN")
		assert.Contains(with, "CREATEDB")
		assert.Contains(with, "NOCREATEROLE")
		assert.Contains(with, "INHERIT")
	})
}

func Test_RoleMeta_Update(t *testing.T) {
	assert := assert.New(t)

	t.Run("UpdateWithSuperuser", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{Name: "testrole", Superuser: types.BoolPtr(true)}
		err := r.Update(bind)
		assert.NoError(err)
		with := bind.Get("with").(string)
		assert.Contains(with, "SUPERUSER")
	})

	t.Run("UpdateWithNoLogin", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{Name: "testrole", Login: types.BoolPtr(false)}
		err := r.Update(bind)
		assert.NoError(err)
		with := bind.Get("with").(string)
		assert.Contains(with, "NOLOGIN")
	})

	t.Run("UpdateEmptyWith", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{Name: "testrole"}
		err := r.Update(bind)
		assert.NoError(err)
		with := bind.Get("with").(string)
		assert.Empty(with)
	})

	t.Run("UpdateDoesNotIncludeGroups", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.RoleMeta{Name: "testrole", Groups: []string{"group1"}}
		err := r.Update(bind)
		assert.NoError(err)
		with := bind.Get("with").(string)
		// Groups are only included for insert, not update
		assert.NotContains(with, "IN ROLE")
	})
}

func Test_RoleListRequest_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("ListOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.RoleListRequest{}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("", bind.Get("where"))
	})

	t.Run("ListWithOffsetLimit", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.RoleListRequest{
			OffsetLimit: pg.OffsetLimit{Offset: 10, Limit: types.Uint64Ptr(20)},
		}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.RoleListRequest{}
		_, err := req.Select(bind, pg.Get)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotImplemented)
	})
}

func Test_Role_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("RoleString", func(t *testing.T) {
		r := schema.Role{
			Oid: 12345,
			RoleMeta: schema.RoleMeta{
				Name:      "testrole",
				Superuser: types.BoolPtr(true),
			},
		}
		str := r.String()
		assert.Contains(str, "testrole")
		assert.Contains(str, "12345")
	})

	t.Run("RoleMetaString", func(t *testing.T) {
		r := schema.RoleMeta{
			Name:  "testrole",
			Login: types.BoolPtr(true),
		}
		str := r.String()
		assert.Contains(str, "testrole")
	})

	t.Run("RoleListString", func(t *testing.T) {
		list := schema.RoleList{
			Count: 2,
			Body: []schema.Role{
				{Oid: 1, RoleMeta: schema.RoleMeta{Name: "role1"}},
				{Oid: 2, RoleMeta: schema.RoleMeta{Name: "role2"}},
			},
		}
		str := list.String()
		assert.Contains(str, "role1")
		assert.Contains(str, "role2")
		assert.Contains(str, `"count": 2`)
	})

	t.Run("RoleListRequestString", func(t *testing.T) {
		req := schema.RoleListRequest{
			OffsetLimit: pg.OffsetLimit{Offset: 5, Limit: types.Uint64Ptr(10)},
		}
		str := req.String()
		// Should be valid JSON
		assert.NotEmpty(str)
	})
}

func Test_RoleList_Scan(t *testing.T) {
	// Note: These are tested via integration tests with real database connections
	// Here we just verify the types exist and have the correct structure
	assert := assert.New(t)

	t.Run("RoleListType", func(t *testing.T) {
		list := schema.RoleList{}
		assert.Equal(uint64(0), list.Count)
		assert.Nil(list.Body)
	})

	t.Run("RoleType", func(t *testing.T) {
		role := schema.Role{}
		assert.Equal(uint32(0), role.Oid)
		assert.Empty(role.Name)
	})
}
