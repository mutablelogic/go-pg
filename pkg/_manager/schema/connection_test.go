package schema_test

import (
	"encoding/json"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_Connection_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidConnection", func(t *testing.T) {
		c := schema.Connection{
			Pid:      12345,
			Database: "testdb",
			Role:     "testuser",
		}
		str := c.String()
		assert.NotEmpty(str)
		assert.Contains(str, "12345")
		assert.Contains(str, "testdb")
		assert.Contains(str, "testuser")
	})

	t.Run("JSONUnmarshal", func(t *testing.T) {
		c := schema.Connection{
			Pid:      12345,
			Database: "testdb",
			Role:     "testuser",
		}
		data, err := json.Marshal(c)
		assert.NoError(err)

		var c2 schema.Connection
		err = json.Unmarshal(data, &c2)
		assert.NoError(err)
		assert.Equal(c.Pid, c2.Pid)
		assert.Equal(c.Database, c2.Database)
		assert.Equal(c.Role, c2.Role)
	})
}

func Test_ConnectionListRequest_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("EmptyRequest", func(t *testing.T) {
		req := schema.ConnectionListRequest{}
		str := req.String()
		assert.NotEmpty(str)
	})

	t.Run("WithFilters", func(t *testing.T) {
		db := "testdb"
		role := "testrole"
		state := "active"
		req := schema.ConnectionListRequest{
			Database: &db,
			Role:     &role,
			State:    &state,
		}
		str := req.String()
		assert.NotEmpty(str)
		assert.Contains(str, "testdb")
		assert.Contains(str, "testrole")
		assert.Contains(str, "active")
	})
}

func Test_ConnectionList_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("EmptyList", func(t *testing.T) {
		list := schema.ConnectionList{Count: 0}
		str := list.String()
		assert.NotEmpty(str)
		assert.Contains(str, "0")
	})

	t.Run("WithConnections", func(t *testing.T) {
		list := schema.ConnectionList{
			Count: 1,
			Body: []schema.Connection{
				{Pid: 12345, Database: "testdb", Role: "testuser"},
			},
		}
		str := list.String()
		assert.NotEmpty(str)
		assert.Contains(str, "12345")
	})
}

func Test_ConnectionListRequest_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("ListOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.ConnectionListRequest{}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
	})

	t.Run("ListWithDatabase", func(t *testing.T) {
		bind := pg.NewBind()
		db := "testdb"
		req := schema.ConnectionListRequest{Database: &db}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		where := bind.Get("where").(string)
		assert.Contains(where, "database")
	})

	t.Run("ListWithRole", func(t *testing.T) {
		bind := pg.NewBind()
		role := "testrole"
		req := schema.ConnectionListRequest{Role: &role}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		where := bind.Get("where").(string)
		assert.Contains(where, "role")
	})

	t.Run("ListWithState", func(t *testing.T) {
		bind := pg.NewBind()
		state := "active"
		req := schema.ConnectionListRequest{State: &state}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		where := bind.Get("where").(string)
		assert.Contains(where, "state")
	})

	t.Run("ListWithAllFilters", func(t *testing.T) {
		bind := pg.NewBind()
		db := "testdb"
		role := "testrole"
		state := "active"
		req := schema.ConnectionListRequest{
			Database: &db,
			Role:     &role,
			State:    &state,
		}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		where := bind.Get("where").(string)
		assert.Contains(where, "AND")
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.ConnectionListRequest{}
		_, err := req.Select(bind, pg.Get)
		assert.Error(err)
	})
}

func Test_ConnectionPid_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("GetOperation", func(t *testing.T) {
		bind := pg.NewBind()
		pid := schema.ConnectionPid(12345)
		sql, err := pid.Select(bind, pg.Get)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal(schema.ConnectionPid(12345), bind.Get("pid"))
	})

	t.Run("DeleteOperation", func(t *testing.T) {
		bind := pg.NewBind()
		pid := schema.ConnectionPid(12345)
		sql, err := pid.Select(bind, pg.Delete)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "pg_terminate_backend")
	})

	t.Run("ZeroPid", func(t *testing.T) {
		bind := pg.NewBind()
		pid := schema.ConnectionPid(0)
		_, err := pid.Select(bind, pg.Get)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		pid := schema.ConnectionPid(12345)
		_, err := pid.Select(bind, pg.Insert)
		assert.Error(err)
	})
}
