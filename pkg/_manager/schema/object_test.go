package schema_test

import (
	"encoding/json"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_ObjectName_Validate(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidSchemaAndName", func(t *testing.T) {
		o := schema.ObjectName{Schema: "public", Name: "users"}
		err := o.Validate()
		assert.NoError(err)
	})

	t.Run("EmptySchema", func(t *testing.T) {
		o := schema.ObjectName{Schema: "", Name: "users"}
		err := o.Validate()
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("WhitespaceOnlySchema", func(t *testing.T) {
		o := schema.ObjectName{Schema: "   ", Name: "users"}
		err := o.Validate()
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("EmptyName", func(t *testing.T) {
		o := schema.ObjectName{Schema: "public", Name: ""}
		err := o.Validate()
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("WhitespaceOnlyName", func(t *testing.T) {
		o := schema.ObjectName{Schema: "public", Name: "   "}
		err := o.Validate()
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("BothEmpty", func(t *testing.T) {
		o := schema.ObjectName{Schema: "", Name: ""}
		err := o.Validate()
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("SchemaWithSpaces", func(t *testing.T) {
		o := schema.ObjectName{Schema: "  public  ", Name: "users"}
		err := o.Validate()
		assert.NoError(err) // Trimmed schema is valid
	})

	t.Run("NameWithSpaces", func(t *testing.T) {
		o := schema.ObjectName{Schema: "public", Name: "  users  "}
		err := o.Validate()
		assert.NoError(err) // Trimmed name is valid
	})
}

func Test_ObjectName_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("GetOperation", func(t *testing.T) {
		bind := pg.NewBind()
		o := schema.ObjectName{Schema: "public", Name: "users"}
		sql, err := o.Select(bind, pg.Get)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("public", bind.Get("schema"))
		assert.Equal("users", bind.Get("name"))
	})

	t.Run("GetOperationWithTrim", func(t *testing.T) {
		bind := pg.NewBind()
		o := schema.ObjectName{Schema: "  myschema  ", Name: "  mytable  "}
		sql, err := o.Select(bind, pg.Get)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("myschema", bind.Get("schema"))
		assert.Equal("mytable", bind.Get("name"))
	})

	t.Run("EmptySchema", func(t *testing.T) {
		bind := pg.NewBind()
		o := schema.ObjectName{Schema: "", Name: "users"}
		_, err := o.Select(bind, pg.Get)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("EmptyName", func(t *testing.T) {
		bind := pg.NewBind()
		o := schema.ObjectName{Schema: "public", Name: ""}
		_, err := o.Select(bind, pg.Get)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("UnsupportedListOperation", func(t *testing.T) {
		bind := pg.NewBind()
		o := schema.ObjectName{Schema: "public", Name: "users"}
		_, err := o.Select(bind, pg.List)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotImplemented)
	})

	t.Run("UnsupportedInsertOperation", func(t *testing.T) {
		bind := pg.NewBind()
		o := schema.ObjectName{Schema: "public", Name: "users"}
		_, err := o.Select(bind, pg.Insert)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotImplemented)
	})

	t.Run("UnsupportedUpdateOperation", func(t *testing.T) {
		bind := pg.NewBind()
		o := schema.ObjectName{Schema: "public", Name: "users"}
		_, err := o.Select(bind, pg.Update)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotImplemented)
	})

	t.Run("UnsupportedDeleteOperation", func(t *testing.T) {
		bind := pg.NewBind()
		o := schema.ObjectName{Schema: "public", Name: "users"}
		_, err := o.Select(bind, pg.Delete)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotImplemented)
	})
}

func Test_ObjectListRequest_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("ListOperationNoFilters", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.ObjectListRequest{}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("ORDER BY schema ASC, name ASC", bind.Get("orderby"))
		assert.Equal("", bind.Get("where"))
	})

	t.Run("ListWithSchemaFilter", func(t *testing.T) {
		bind := pg.NewBind()
		schemaName := "public"
		req := schema.ObjectListRequest{Schema: &schemaName}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		where := bind.Get("where").(string)
		assert.Contains(where, "schema = ")
		assert.Contains(where, "public")
	})

	t.Run("ListWithDatabaseFilter", func(t *testing.T) {
		bind := pg.NewBind()
		database := "mydb"
		req := schema.ObjectListRequest{Database: &database}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		where := bind.Get("where").(string)
		assert.Contains(where, "database = ")
		assert.Contains(where, "mydb")
	})

	t.Run("ListWithTypeFilter", func(t *testing.T) {
		bind := pg.NewBind()
		objectType := "TABLE"
		req := schema.ObjectListRequest{Type: &objectType}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		where := bind.Get("where").(string)
		assert.Contains(where, "type = ")
		assert.Contains(where, "TABLE")
	})

	t.Run("ListWithMultipleFilters", func(t *testing.T) {
		bind := pg.NewBind()
		schemaName := "public"
		database := "mydb"
		objectType := "VIEW"
		req := schema.ObjectListRequest{
			Schema:   &schemaName,
			Database: &database,
			Type:     &objectType,
		}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		where := bind.Get("where").(string)
		assert.Contains(where, "schema = ")
		assert.Contains(where, "database = ")
		assert.Contains(where, "type = ")
		assert.Contains(where, " AND ")
	})

	t.Run("ListWithEmptyStringFilters", func(t *testing.T) {
		bind := pg.NewBind()
		emptySchema := ""
		emptyType := "   "
		req := schema.ObjectListRequest{
			Schema: &emptySchema,
			Type:   &emptyType,
		}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		// Empty/whitespace strings should not add where clauses
		assert.Equal("", bind.Get("where"))
	})

	t.Run("ListWithOffsetLimit", func(t *testing.T) {
		bind := pg.NewBind()
		limit := uint64(25)
		req := schema.ObjectListRequest{
			OffsetLimit: pg.OffsetLimit{
				Offset: 10,
				Limit:  &limit,
			},
		}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		offsetlimit := bind.Get("offsetlimit").(string)
		assert.Contains(offsetlimit, "OFFSET 10")
		assert.Contains(offsetlimit, "LIMIT 25")
	})

	t.Run("UnsupportedGetOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.ObjectListRequest{}
		_, err := req.Select(bind, pg.Get)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotImplemented)
	})

	t.Run("UnsupportedInsertOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.ObjectListRequest{}
		_, err := req.Select(bind, pg.Insert)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotImplemented)
	})
}

func Test_ObjectName_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidJSON", func(t *testing.T) {
		o := schema.ObjectName{Schema: "public", Name: "users"}
		str := o.String()
		assert.NotEmpty(str)

		var parsed map[string]interface{}
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal("public", parsed["schema"])
		assert.Equal("users", parsed["name"])
	})
}

func Test_ObjectMeta_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidJSON", func(t *testing.T) {
		o := schema.ObjectMeta{Name: "users", Owner: "postgres"}
		str := o.String()
		assert.NotEmpty(str)

		var parsed map[string]interface{}
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal("users", parsed["name"])
		assert.Equal("postgres", parsed["owner"])
	})
}

func Test_Object_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidJSON", func(t *testing.T) {
		tablespace := "pg_default"
		o := schema.Object{
			Oid:      12345,
			Database: "mydb",
			Schema:   "public",
			Type:     "TABLE",
			ObjectMeta: schema.ObjectMeta{
				Name:  "users",
				Owner: "postgres",
			},
			Tablespace: &tablespace,
			Size:       1024,
		}
		str := o.String()
		assert.NotEmpty(str)

		var parsed map[string]interface{}
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal(float64(12345), parsed["oid"])
		assert.Equal("mydb", parsed["database"])
		assert.Equal("public", parsed["schema"])
		assert.Equal("TABLE", parsed["type"])
		assert.Equal("users", parsed["name"])
		assert.Equal("postgres", parsed["owner"])
		assert.Equal("pg_default", parsed["tablespace"])
		assert.Equal(float64(1024), parsed["bytes"])
	})

	t.Run("NilTablespace", func(t *testing.T) {
		o := schema.Object{
			Oid:      12345,
			Database: "mydb",
			Schema:   "public",
			Type:     "VIEW",
			ObjectMeta: schema.ObjectMeta{
				Name:  "user_view",
				Owner: "postgres",
			},
			Tablespace: nil,
			Size:       0,
		}
		str := o.String()
		assert.NotEmpty(str)

		var parsed map[string]interface{}
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		_, hasTablespace := parsed["tablespace"]
		assert.False(hasTablespace) // omitempty should exclude nil tablespace
	})
}

func Test_ObjectList_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidJSON", func(t *testing.T) {
		list := schema.ObjectList{
			Count: 2,
			Body: []schema.Object{
				{
					Oid:      1,
					Database: "db1",
					Schema:   "public",
					Type:     "TABLE",
					ObjectMeta: schema.ObjectMeta{
						Name:  "table1",
						Owner: "owner1",
					},
				},
				{
					Oid:      2,
					Database: "db1",
					Schema:   "public",
					Type:     "VIEW",
					ObjectMeta: schema.ObjectMeta{
						Name:  "view1",
						Owner: "owner2",
					},
				},
			},
		}
		str := list.String()
		assert.NotEmpty(str)

		var parsed map[string]interface{}
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal(float64(2), parsed["count"])
		body := parsed["body"].([]interface{})
		assert.Len(body, 2)
	})

	t.Run("EmptyBody", func(t *testing.T) {
		list := schema.ObjectList{
			Count: 0,
			Body:  []schema.Object{},
		}
		str := list.String()
		assert.NotEmpty(str)

		var parsed map[string]interface{}
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal(float64(0), parsed["count"])
	})
}

func Test_ObjectListRequest_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidJSON", func(t *testing.T) {
		schemaName := "public"
		database := "mydb"
		objectType := "TABLE"
		req := schema.ObjectListRequest{
			Schema:   &schemaName,
			Database: &database,
			Type:     &objectType,
		}
		str := req.String()
		assert.NotEmpty(str)

		var parsed map[string]interface{}
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal("public", parsed["schema"])
		assert.Equal("mydb", parsed["database"])
		assert.Equal("TABLE", parsed["type"])
	})

	t.Run("NilFields", func(t *testing.T) {
		req := schema.ObjectListRequest{}
		str := req.String()
		assert.NotEmpty(str)

		var parsed map[string]interface{}
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		_, hasSchema := parsed["schema"]
		_, hasDatabase := parsed["database"]
		_, hasType := parsed["type"]
		assert.False(hasSchema)
		assert.False(hasDatabase)
		assert.False(hasType)
	})
}

func Test_TableMeta_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("WithValues", func(t *testing.T) {
		live := int64(1000)
		dead := int64(50)
		tm := schema.TableMeta{
			LiveTuples: &live,
			DeadTuples: &dead,
		}
		str := tm.String()
		assert.NotEmpty(str)

		var parsed map[string]interface{}
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal(float64(1000), parsed["live_tuples"])
		assert.Equal(float64(50), parsed["dead_tuples"])
	})

	t.Run("NilValues", func(t *testing.T) {
		tm := schema.TableMeta{}
		str := tm.String()
		assert.NotEmpty(str)

		var parsed map[string]interface{}
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		_, hasLive := parsed["live_tuples"]
		_, hasDead := parsed["dead_tuples"]
		assert.False(hasLive)
		assert.False(hasDead)
	})

	t.Run("OnlyLiveTuples", func(t *testing.T) {
		live := int64(500)
		tm := schema.TableMeta{
			LiveTuples: &live,
		}
		str := tm.String()

		var parsed map[string]interface{}
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal(float64(500), parsed["live_tuples"])
		_, hasDead := parsed["dead_tuples"]
		assert.False(hasDead)
	})

	t.Run("ZeroValues", func(t *testing.T) {
		live := int64(0)
		dead := int64(0)
		tm := schema.TableMeta{
			LiveTuples: &live,
			DeadTuples: &dead,
		}
		str := tm.String()

		var parsed map[string]interface{}
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal(float64(0), parsed["live_tuples"])
		assert.Equal(float64(0), parsed["dead_tuples"])
	})
}

func Test_Object_WithTableMeta(t *testing.T) {
	assert := assert.New(t)

	t.Run("TableWithMeta", func(t *testing.T) {
		live := int64(1000)
		dead := int64(50)
		o := schema.Object{
			Oid:      16384,
			Database: "mydb",
			Schema:   "public",
			Type:     "TABLE",
			ObjectMeta: schema.ObjectMeta{
				Name:  "users",
				Owner: "postgres",
			},
			Size: 8192,
			Table: &schema.TableMeta{
				LiveTuples: &live,
				DeadTuples: &dead,
			},
		}
		str := o.String()
		assert.NotEmpty(str)

		var parsed map[string]interface{}
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal("TABLE", parsed["type"])
		assert.Equal("users", parsed["name"])

		table, hasTable := parsed["table"].(map[string]interface{})
		assert.True(hasTable)
		assert.Equal(float64(1000), table["live_tuples"])
		assert.Equal(float64(50), table["dead_tuples"])
	})

	t.Run("IndexWithoutTableMeta", func(t *testing.T) {
		o := schema.Object{
			Oid:      16385,
			Database: "mydb",
			Schema:   "public",
			Type:     "INDEX",
			ObjectMeta: schema.ObjectMeta{
				Name:  "users_pkey",
				Owner: "postgres",
			},
			Size:  4096,
			Table: nil,
		}
		str := o.String()
		assert.NotEmpty(str)

		var parsed map[string]interface{}
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal("INDEX", parsed["type"])
		assert.Equal("users_pkey", parsed["name"])

		_, hasTable := parsed["table"]
		assert.False(hasTable, "INDEX should not have table metadata")
	})

	t.Run("ViewWithoutTableMeta", func(t *testing.T) {
		o := schema.Object{
			Oid:      16386,
			Database: "mydb",
			Schema:   "public",
			Type:     "VIEW",
			ObjectMeta: schema.ObjectMeta{
				Name:  "active_users",
				Owner: "postgres",
			},
			Table: nil,
		}
		str := o.String()

		var parsed map[string]interface{}
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal("VIEW", parsed["type"])
		_, hasTable := parsed["table"]
		assert.False(hasTable, "VIEW should not have table metadata")
	})
}
