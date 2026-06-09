package schema_test

import (
	"encoding/json"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_DatabaseMeta_Validate(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidNameAndOwner", func(t *testing.T) {
		d := schema.DatabaseMeta{Name: "testdb", Owner: "testuser"}
		err := d.Validate()
		assert.NoError(err)
	})

	t.Run("ValidNameNoOwner", func(t *testing.T) {
		d := schema.DatabaseMeta{Name: "testdb"}
		err := d.Validate()
		assert.NoError(err)
	})

	t.Run("EmptyName", func(t *testing.T) {
		d := schema.DatabaseMeta{Name: "", Owner: "testuser"}
		err := d.Validate()
		assert.Error(err)
	})

	t.Run("WhitespaceOnlyName", func(t *testing.T) {
		d := schema.DatabaseMeta{Name: "   ", Owner: "testuser"}
		err := d.Validate()
		assert.Error(err)
	})

	t.Run("ReservedPrefixName", func(t *testing.T) {
		d := schema.DatabaseMeta{Name: "pg_testdb", Owner: "testuser"}
		err := d.Validate()
		assert.Error(err)
	})

	t.Run("ReservedPrefixOwner", func(t *testing.T) {
		d := schema.DatabaseMeta{Name: "testdb", Owner: "pg_admin"}
		err := d.Validate()
		assert.Error(err)
	})

	t.Run("NameWithSpaces", func(t *testing.T) {
		d := schema.DatabaseMeta{Name: "  testdb  ", Owner: "testuser"}
		err := d.Validate()
		assert.NoError(err) // Trimmed name is valid
	})
}

func Test_DatabaseName_name(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidName", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseName("testdb")
		_, err := d.Select(bind, pg.Get)
		assert.NoError(err)
		assert.Equal("testdb", bind.Get("name"))
	})

	t.Run("EmptyName", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseName("")
		_, err := d.Select(bind, pg.Get)
		assert.Error(err)
	})

	t.Run("ReservedPrefix", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseName("pg_mydb")
		_, err := d.Select(bind, pg.Get)
		assert.Error(err)
	})
}

func Test_DatabaseListRequest_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("ListOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.DatabaseListRequest{}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("ORDER BY name ASC", bind.Get("orderby"))
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.DatabaseListRequest{}
		_, err := req.Select(bind, pg.Get)
		assert.Error(err)
	})
}

func Test_DatabaseName_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("GetOperation", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseName("mydb")
		sql, err := d.Select(bind, pg.Get)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("mydb", bind.Get("name"))
	})

	t.Run("UpdateOperation", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseName("mydb")
		sql, err := d.Select(bind, pg.Update)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "RENAME")
	})

	t.Run("DeleteOperation", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseName("mydb")
		sql, err := d.Select(bind, pg.Delete)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "DROP")
	})

	t.Run("DeleteWithForce", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("force", true)
		d := schema.DatabaseName("mydb")
		_, err := d.Select(bind, pg.Delete)
		assert.NoError(err)
		assert.Equal("(FORCE)", bind.Get("with"))
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseName("mydb")
		_, err := d.Select(bind, pg.Insert)
		assert.Error(err)
	})
}

func Test_DatabaseMeta_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("UpdateOperation", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseMeta{Name: "mydb", Owner: "newowner"}
		sql, err := d.Select(bind, pg.Update)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("mydb", bind.Get("name"))
	})

	t.Run("EmptyName", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseMeta{Name: "", Owner: "newowner"}
		_, err := d.Select(bind, pg.Update)
		assert.Error(err)
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseMeta{Name: "mydb"}
		_, err := d.Select(bind, pg.Get)
		assert.Error(err)
	})
}

func Test_DatabaseMeta_Insert(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidInsert", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseMeta{Name: "newdb", Owner: "admin"}
		sql, err := d.Insert(bind)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "CREATE DATABASE")
		assert.Equal("newdb", bind.Get("name"))
	})

	t.Run("InsertWithoutOwner", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseMeta{Name: "newdb"}
		sql, err := d.Insert(bind)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("", bind.Get("with"))
	})

	t.Run("InsertWithOwner", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseMeta{Name: "newdb", Owner: "myowner"}
		_, err := d.Insert(bind)
		assert.NoError(err)
		with := bind.Get("with").(string)
		assert.Contains(with, "WITH OWNER")
		assert.Contains(with, "myowner")
	})

	t.Run("InvalidName", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseMeta{Name: ""}
		_, err := d.Insert(bind)
		assert.Error(err)
	})

	t.Run("ReservedPrefixName", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseMeta{Name: "pg_mydb"}
		_, err := d.Insert(bind)
		assert.Error(err)
	})

	t.Run("ReservedPrefixOwner", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseMeta{Name: "mydb", Owner: "pg_admin"}
		_, err := d.Insert(bind)
		assert.Error(err)
	})
}

func Test_DatabaseMeta_Update(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidUpdate", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseMeta{Name: "mydb", Owner: "newowner"}
		err := d.Update(bind)
		assert.NoError(err)
		with := bind.Get("with").(string)
		assert.Contains(with, "OWNER TO")
	})

	t.Run("UpdateNoOwner", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseMeta{Name: "mydb"}
		err := d.Update(bind)
		assert.NoError(err)
		assert.Equal("", bind.Get("with"))
	})

	t.Run("ReservedPrefixOwner", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseMeta{Name: "mydb", Owner: "pg_system"}
		err := d.Update(bind)
		assert.Error(err)
	})
}

func Test_DatabaseName_Insert(t *testing.T) {
	assert := assert.New(t)

	t.Run("NotImplemented", func(t *testing.T) {
		bind := pg.NewBind()
		d := schema.DatabaseName("mydb")
		_, err := d.Insert(bind)
		assert.Error(err)
	})
}

func Test_DatabaseName_Update(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidRename", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("name", "newname")
		d := schema.DatabaseName("oldname")
		err := d.Update(bind)
		assert.NoError(err)
		assert.Equal("oldname", bind.Get("old_name"))
	})

	t.Run("InvalidOldName", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("name", "newname")
		d := schema.DatabaseName("")
		err := d.Update(bind)
		assert.Error(err)
	})

	t.Run("ReservedPrefixOldName", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("name", "newname")
		d := schema.DatabaseName("pg_olddb")
		err := d.Update(bind)
		assert.Error(err)
	})

	t.Run("ReservedPrefixNewName", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("name", "pg_newdb")
		d := schema.DatabaseName("oldname")
		err := d.Update(bind)
		assert.Error(err)
	})

	t.Run("EmptyNewName", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("name", "")
		d := schema.DatabaseName("oldname")
		err := d.Update(bind)
		assert.Error(err)
	})
}

func Test_Database_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ProducesJSON", func(t *testing.T) {
		d := schema.Database{
			Oid: 12345,
			DatabaseMeta: schema.DatabaseMeta{
				Name:  "testdb",
				Owner: "admin",
			},
			Size: 1024000,
		}
		str := d.String()
		assert.Contains(str, "testdb")
		assert.Contains(str, "admin")
		// Should be valid JSON
		var parsed map[string]any
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
	})
}

func Test_DatabaseMeta_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ProducesJSON", func(t *testing.T) {
		d := schema.DatabaseMeta{Name: "testdb", Owner: "admin"}
		str := d.String()
		assert.Contains(str, "testdb")
		var parsed map[string]any
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
	})
}

func Test_DatabaseList_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ProducesJSON", func(t *testing.T) {
		list := schema.DatabaseList{
			Count: 2,
			Body: []schema.Database{
				{Oid: 1, DatabaseMeta: schema.DatabaseMeta{Name: "db1"}},
				{Oid: 2, DatabaseMeta: schema.DatabaseMeta{Name: "db2"}},
			},
		}
		str := list.String()
		assert.Contains(str, "db1")
		assert.Contains(str, "db2")
		var parsed map[string]any
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
	})
}

func Test_DatabaseListRequest_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ProducesJSON", func(t *testing.T) {
		req := schema.DatabaseListRequest{}
		str := req.String()
		var parsed map[string]any
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
	})
}
