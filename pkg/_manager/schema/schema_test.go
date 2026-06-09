package schema_test

import (
	"encoding/json"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_SchemaName_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("GetOperation", func(t *testing.T) {
		bind := pg.NewBind()
		s := schema.SchemaName("public")
		sql, err := s.Select(bind, pg.Get)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("public", bind.Get("name"))
	})

	t.Run("UpdateOperation", func(t *testing.T) {
		bind := pg.NewBind()
		s := schema.SchemaName("myschema")
		sql, err := s.Select(bind, pg.Update)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "RENAME")
	})

	t.Run("DeleteOperation", func(t *testing.T) {
		bind := pg.NewBind()
		s := schema.SchemaName("myschema")
		sql, err := s.Select(bind, pg.Delete)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "DROP")
	})

	t.Run("DeleteWithForce", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("force", true)
		s := schema.SchemaName("myschema")
		_, err := s.Select(bind, pg.Delete)
		assert.NoError(err)
		assert.Equal("CASCADE", bind.Get("with"))
	})

	t.Run("EmptyName", func(t *testing.T) {
		bind := pg.NewBind()
		s := schema.SchemaName("")
		_, err := s.Select(bind, pg.Get)
		assert.Error(err)
	})

	t.Run("WhitespaceOnlyName", func(t *testing.T) {
		bind := pg.NewBind()
		s := schema.SchemaName("   ")
		_, err := s.Select(bind, pg.Get)
		assert.Error(err)
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		s := schema.SchemaName("myschema")
		_, err := s.Select(bind, pg.Insert)
		assert.Error(err)
	})
}

func Test_SchemaListRequest_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("ListOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.SchemaListRequest{}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("ORDER BY name ASC", bind.Get("orderby"))
	})

	t.Run("ListWithDatabase", func(t *testing.T) {
		bind := pg.NewBind()
		db := "testdb"
		req := schema.SchemaListRequest{Database: &db}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		where := bind.Get("where").(string)
		assert.Contains(where, "database")
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.SchemaListRequest{}
		_, err := req.Select(bind, pg.Get)
		assert.Error(err)
	})
}

func Test_SchemaMeta_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("UpdateOperation", func(t *testing.T) {
		bind := pg.NewBind()
		s := schema.SchemaMeta{Name: "myschema", Owner: "newowner"}
		sql, err := s.Select(bind, pg.Update)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("myschema", bind.Get("name"))
	})

	t.Run("EmptyName", func(t *testing.T) {
		bind := pg.NewBind()
		s := schema.SchemaMeta{Name: "", Owner: "newowner"}
		_, err := s.Select(bind, pg.Update)
		assert.Error(err)
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		s := schema.SchemaMeta{Name: "myschema"}
		_, err := s.Select(bind, pg.Get)
		assert.Error(err)
	})
}

func Test_SchemaMeta_Insert(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidInsert", func(t *testing.T) {
		bind := pg.NewBind()
		s := schema.SchemaMeta{Name: "newschema", Owner: "admin"}
		sql, err := s.Insert(bind)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "CREATE SCHEMA")
		assert.Equal("newschema", bind.Get("name"))
	})

	t.Run("InsertWithOwner", func(t *testing.T) {
		bind := pg.NewBind()
		s := schema.SchemaMeta{Name: "newschema", Owner: "myowner"}
		_, err := s.Insert(bind)
		assert.NoError(err)
		with := bind.Get("with").(string)
		assert.Contains(with, "AUTHORIZATION")
		assert.Contains(with, "myowner")
	})

	t.Run("InsertWithoutOwner", func(t *testing.T) {
		bind := pg.NewBind()
		s := schema.SchemaMeta{Name: "newschema"}
		sql, err := s.Insert(bind)
		assert.NoError(err) // Owner is optional for schema creation
		assert.NotEmpty(sql)
		assert.Contains(sql, "CREATE SCHEMA")
		assert.NotContains(sql, "AUTHORIZATION") // No owner means no AUTHORIZATION clause
	})

	t.Run("InvalidName", func(t *testing.T) {
		bind := pg.NewBind()
		s := schema.SchemaMeta{Name: "", Owner: "admin"}
		_, err := s.Insert(bind)
		assert.Error(err)
	})

	t.Run("ReservedPrefixName", func(t *testing.T) {
		bind := pg.NewBind()
		s := schema.SchemaMeta{Name: "pg_myschema", Owner: "admin"}
		_, err := s.Insert(bind)
		assert.Error(err)
	})
}

func Test_SchemaMeta_Update(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidUpdate", func(t *testing.T) {
		bind := pg.NewBind()
		s := schema.SchemaMeta{Name: "myschema", Owner: "newowner"}
		err := s.Update(bind)
		assert.NoError(err)
		with := bind.Get("with").(string)
		assert.Contains(with, "OWNER TO")
	})

	t.Run("UpdateNoChanges", func(t *testing.T) {
		bind := pg.NewBind()
		s := schema.SchemaMeta{Name: "myschema"}
		err := s.Update(bind)
		assert.Error(err) // Should error when no changes specified
	})
}

func Test_SchemaName_Insert(t *testing.T) {
	assert := assert.New(t)

	t.Run("NotImplemented", func(t *testing.T) {
		bind := pg.NewBind()
		s := schema.SchemaName("myschema")
		_, err := s.Insert(bind)
		assert.Error(err)
	})
}

func Test_SchemaName_Update(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidRename", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("name", "newname")
		s := schema.SchemaName("oldname")
		err := s.Update(bind)
		assert.NoError(err)
		assert.Equal("oldname", bind.Get("old_name"))
	})

	t.Run("InvalidOldName", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("name", "newname")
		s := schema.SchemaName("")
		err := s.Update(bind)
		assert.Error(err)
	})

	t.Run("ReservedPrefixOldName", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("name", "newname")
		s := schema.SchemaName("pg_oldschema")
		err := s.Update(bind)
		assert.Error(err)
	})

	t.Run("PublicSchemaRename", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("name", "newname")
		s := schema.SchemaName("public")
		err := s.Update(bind)
		assert.Error(err) // Cannot rename default schema
	})
}

func Test_Schema_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ProducesJSON", func(t *testing.T) {
		s := schema.Schema{
			Oid:      12345,
			Database: "testdb",
			SchemaMeta: schema.SchemaMeta{
				Name:  "myschema",
				Owner: "admin",
			},
			Size: 1024000,
		}
		str := s.String()
		assert.Contains(str, "myschema")
		assert.Contains(str, "admin")
		// Should be valid JSON
		var parsed map[string]any
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
	})
}

func Test_SchemaMeta_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ProducesJSON", func(t *testing.T) {
		s := schema.SchemaMeta{Name: "myschema", Owner: "admin"}
		str := s.String()
		assert.Contains(str, "myschema")
		var parsed map[string]any
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
	})
}

func Test_SchemaList_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ProducesJSON", func(t *testing.T) {
		list := schema.SchemaList{
			Count: 2,
			Body: []schema.Schema{
				{Oid: 1, SchemaMeta: schema.SchemaMeta{Name: "schema1"}},
				{Oid: 2, SchemaMeta: schema.SchemaMeta{Name: "schema2"}},
			},
		}
		str := list.String()
		assert.Contains(str, "schema1")
		assert.Contains(str, "schema2")
		var parsed map[string]any
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
	})
}

func Test_SchemaListRequest_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ProducesJSON", func(t *testing.T) {
		req := schema.SchemaListRequest{}
		str := req.String()
		var parsed map[string]any
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
	})

	t.Run("WithDatabase", func(t *testing.T) {
		db := "testdb"
		req := schema.SchemaListRequest{Database: &db}
		str := req.String()
		assert.Contains(str, "testdb")
		var parsed map[string]any
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
	})
}
