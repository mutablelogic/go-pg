package schema_test

import (
	"encoding/json"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_Tablespace_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidTablespace", func(t *testing.T) {
		ts := schema.Tablespace{
			Oid: 12345,
			TablespaceMeta: schema.TablespaceMeta{
				Name:  "mytablespace",
				Owner: "testuser",
			},
			Location: "/data/tablespace",
		}
		str := ts.String()
		assert.NotEmpty(str)
		assert.Contains(str, "mytablespace")
		assert.Contains(str, "testuser")
	})

	t.Run("JSONUnmarshal", func(t *testing.T) {
		ts := schema.Tablespace{
			Oid: 12345,
			TablespaceMeta: schema.TablespaceMeta{
				Name:  "mytablespace",
				Owner: "testuser",
			},
			Location: "/data/tablespace",
		}
		data, err := json.Marshal(ts)
		assert.NoError(err)

		var ts2 schema.Tablespace
		err = json.Unmarshal(data, &ts2)
		assert.NoError(err)
		assert.Equal(ts.Oid, ts2.Oid)
		assert.Equal(ts.Name, ts2.Name)
		assert.Equal(ts.Owner, ts2.Owner)
		assert.Equal(ts.Location, ts2.Location)
	})
}

func Test_TablespaceMeta_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidMeta", func(t *testing.T) {
		meta := schema.TablespaceMeta{
			Name:  "mytablespace",
			Owner: "testuser",
		}
		str := meta.String()
		assert.NotEmpty(str)
		assert.Contains(str, "mytablespace")
		assert.Contains(str, "testuser")
	})
}

func Test_TablespaceListRequest_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("EmptyRequest", func(t *testing.T) {
		req := schema.TablespaceListRequest{}
		str := req.String()
		assert.NotEmpty(str)
	})
}

func Test_TablespaceList_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("EmptyList", func(t *testing.T) {
		list := schema.TablespaceList{Count: 0}
		str := list.String()
		assert.NotEmpty(str)
		assert.Contains(str, "0")
	})

	t.Run("WithTablespaces", func(t *testing.T) {
		list := schema.TablespaceList{
			Count: 1,
			Body: []schema.Tablespace{
				{
					Oid: 12345,
					TablespaceMeta: schema.TablespaceMeta{
						Name: "mytablespace",
					},
				},
			},
		}
		str := list.String()
		assert.NotEmpty(str)
		assert.Contains(str, "mytablespace")
	})
}

func Test_TablespaceListRequest_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("ListOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.TablespaceListRequest{}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("ORDER BY name ASC", bind.Get("orderby"))
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.TablespaceListRequest{}
		_, err := req.Select(bind, pg.Get)
		assert.Error(err)
	})
}

func Test_TablespaceName_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("GetOperation", func(t *testing.T) {
		bind := pg.NewBind()
		ts := schema.TablespaceName("mytablespace")
		sql, err := ts.Select(bind, pg.Get)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("mytablespace", bind.Get("name"))
	})

	t.Run("UpdateOperation", func(t *testing.T) {
		bind := pg.NewBind()
		ts := schema.TablespaceName("mytablespace")
		sql, err := ts.Select(bind, pg.Update)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "RENAME")
	})

	t.Run("DeleteOperation", func(t *testing.T) {
		bind := pg.NewBind()
		ts := schema.TablespaceName("mytablespace")
		sql, err := ts.Select(bind, pg.Delete)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "DROP")
	})

	t.Run("EmptyName", func(t *testing.T) {
		bind := pg.NewBind()
		ts := schema.TablespaceName("")
		_, err := ts.Select(bind, pg.Get)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("WhitespaceOnlyName", func(t *testing.T) {
		bind := pg.NewBind()
		ts := schema.TablespaceName("   ")
		_, err := ts.Select(bind, pg.Get)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		ts := schema.TablespaceName("mytablespace")
		_, err := ts.Select(bind, pg.Insert)
		assert.Error(err)
	})
}

func Test_TablespaceMeta_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("UpdateOperation", func(t *testing.T) {
		bind := pg.NewBind()
		meta := schema.TablespaceMeta{Name: "mytablespace", Owner: "newowner"}
		sql, err := meta.Select(bind, pg.Update)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("mytablespace", bind.Get("name"))
	})

	t.Run("EmptyName", func(t *testing.T) {
		bind := pg.NewBind()
		meta := schema.TablespaceMeta{Name: "", Owner: "newowner"}
		_, err := meta.Select(bind, pg.Update)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		meta := schema.TablespaceMeta{Name: "mytablespace"}
		_, err := meta.Select(bind, pg.Get)
		assert.Error(err)
	})
}

func Test_TablespaceMeta_Insert(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidInsert", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("location", "/data/tablespace")
		meta := schema.TablespaceMeta{Name: "newtablespace", Owner: "admin"}
		sql, err := meta.Insert(bind)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "CREATE TABLESPACE")
		assert.Equal("newtablespace", bind.Get("name"))
	})

	t.Run("InsertWithOwner", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("location", "/data/tablespace")
		meta := schema.TablespaceMeta{Name: "newtablespace", Owner: "myowner"}
		_, err := meta.Insert(bind)
		assert.NoError(err)
		with := bind.Get("with").(string)
		assert.Contains(with, "OWNER")
	})

	t.Run("EmptyName", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("location", "/data/tablespace")
		meta := schema.TablespaceMeta{Name: "", Owner: "admin"}
		_, err := meta.Insert(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("ReservedPrefixName", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("location", "/data/tablespace")
		meta := schema.TablespaceMeta{Name: "pg_mytablespace", Owner: "admin"}
		_, err := meta.Insert(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("MissingLocation", func(t *testing.T) {
		bind := pg.NewBind()
		meta := schema.TablespaceMeta{Name: "newtablespace", Owner: "admin"}
		_, err := meta.Insert(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("EmptyLocation", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("location", "")
		meta := schema.TablespaceMeta{Name: "newtablespace", Owner: "admin"}
		_, err := meta.Insert(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("RelativeLocation", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("location", "data/tablespace")
		meta := schema.TablespaceMeta{Name: "newtablespace", Owner: "admin"}
		_, err := meta.Insert(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})
}

func Test_TablespaceName_Insert(t *testing.T) {
	assert := assert.New(t)

	t.Run("NotImplemented", func(t *testing.T) {
		bind := pg.NewBind()
		ts := schema.TablespaceName("mytablespace")
		_, err := ts.Insert(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotImplemented)
	})
}

func Test_TablespaceName_Update(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidUpdate", func(t *testing.T) {
		bind := pg.NewBind()
		ts := schema.TablespaceName("mytablespace")
		err := ts.Update(bind)
		assert.NoError(err)
		assert.Equal("mytablespace", bind.Get("old_name"))
	})

	t.Run("EmptyName", func(t *testing.T) {
		bind := pg.NewBind()
		ts := schema.TablespaceName("")
		err := ts.Update(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("ReservedPrefixName", func(t *testing.T) {
		bind := pg.NewBind()
		ts := schema.TablespaceName("pg_mytablespace")
		err := ts.Update(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})
}
