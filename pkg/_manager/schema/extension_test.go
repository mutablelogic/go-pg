package schema_test

import (
	"encoding/json"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_Extension_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidExtension", func(t *testing.T) {
		oid := uint32(12345)
		ext := schema.Extension{
			Oid: &oid,
			ExtensionMeta: schema.ExtensionMeta{
				Name:  "postgis",
				Owner: "postgres",
			},
			DefaultVersion: "3.3.0",
			Comment:        "PostGIS geometry and geography types",
		}
		str := ext.String()
		assert.NotEmpty(str)
		assert.Contains(str, "postgis")
		assert.Contains(str, "postgres")
	})

	t.Run("JSONUnmarshal", func(t *testing.T) {
		oid := uint32(12345)
		installedVersion := "3.3.0"
		relocatable := true
		ext := schema.Extension{
			Oid: &oid,
			ExtensionMeta: schema.ExtensionMeta{
				Name:   "postgis",
				Owner:  "postgres",
				Schema: "public",
			},
			DefaultVersion:   "3.3.0",
			InstalledVersion: &installedVersion,
			Relocatable:      &relocatable,
			Comment:          "PostGIS geometry",
			Requires:         []string{"plpgsql"},
		}
		data, err := json.Marshal(ext)
		assert.NoError(err)

		var ext2 schema.Extension
		err = json.Unmarshal(data, &ext2)
		assert.NoError(err)
		assert.Equal(*ext.Oid, *ext2.Oid)
		assert.Equal(ext.Name, ext2.Name)
		assert.Equal(ext.Owner, ext2.Owner)
		assert.Equal(ext.Schema, ext2.Schema)
		assert.Equal(ext.DefaultVersion, ext2.DefaultVersion)
		assert.Equal(*ext.InstalledVersion, *ext2.InstalledVersion)
		assert.Equal(*ext.Relocatable, *ext2.Relocatable)
		assert.Equal(ext.Requires, ext2.Requires)
	})

	t.Run("NotInstalledExtension", func(t *testing.T) {
		ext := schema.Extension{
			ExtensionMeta: schema.ExtensionMeta{
				Name: "postgis",
			},
			DefaultVersion: "3.3.0",
		}
		str := ext.String()
		assert.NotEmpty(str)
		assert.Contains(str, "postgis")
		// Oid and InstalledVersion should be nil
		assert.Nil(ext.Oid)
		assert.Nil(ext.InstalledVersion)
	})
}

func Test_ExtensionMeta_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidMeta", func(t *testing.T) {
		meta := schema.ExtensionMeta{
			Name:    "postgis",
			Schema:  "public",
			Version: "3.3.0",
		}
		str := meta.String()
		assert.NotEmpty(str)
		assert.Contains(str, "postgis")
		assert.Contains(str, "public")
	})
}

func Test_ExtensionListRequest_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("EmptyRequest", func(t *testing.T) {
		req := schema.ExtensionListRequest{}
		str := req.String()
		assert.NotEmpty(str)
	})

	t.Run("WithInstalledFilter", func(t *testing.T) {
		installed := true
		req := schema.ExtensionListRequest{Installed: &installed}
		str := req.String()
		assert.NotEmpty(str)
		assert.Contains(str, "true")
	})
}

func Test_ExtensionList_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("EmptyList", func(t *testing.T) {
		list := schema.ExtensionList{Count: 0}
		str := list.String()
		assert.NotEmpty(str)
		assert.Contains(str, "0")
	})

	t.Run("WithExtensions", func(t *testing.T) {
		list := schema.ExtensionList{
			Count: 1,
			Body: []schema.Extension{
				{
					ExtensionMeta: schema.ExtensionMeta{
						Name: "postgis",
					},
					DefaultVersion: "3.3.0",
				},
			},
		}
		str := list.String()
		assert.NotEmpty(str)
		assert.Contains(str, "postgis")
	})
}

func Test_ExtensionListRequest_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("ListOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.ExtensionListRequest{}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("ORDER BY name ASC", bind.Get("orderby"))
		assert.Equal("", bind.Get("where"))
	})

	t.Run("ListInstalledOnly", func(t *testing.T) {
		bind := pg.NewBind()
		installed := true
		req := schema.ExtensionListRequest{Installed: &installed}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		where := bind.Get("where").(string)
		assert.Contains(where, "installed_version IS NOT NULL")
	})

	t.Run("ListNotInstalledOnly", func(t *testing.T) {
		bind := pg.NewBind()
		installed := false
		req := schema.ExtensionListRequest{Installed: &installed}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		where := bind.Get("where").(string)
		assert.Contains(where, "installed_version IS NULL")
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.ExtensionListRequest{}
		_, err := req.Select(bind, pg.Get)
		assert.Error(err)
	})
}

func Test_ExtensionName_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("GetOperation", func(t *testing.T) {
		bind := pg.NewBind()
		ext := schema.ExtensionName("postgis")
		sql, err := ext.Select(bind, pg.Get)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("postgis", bind.Get("name"))
	})

	t.Run("DeleteOperation", func(t *testing.T) {
		bind := pg.NewBind()
		ext := schema.ExtensionName("postgis")
		sql, err := ext.Select(bind, pg.Delete)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "DROP")
		assert.Equal("postgis", bind.Get("name")) // Raw name - template handles quoting
		assert.Equal("", bind.Get("cascade"))
	})

	t.Run("DeleteWithCascade", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("cascade", true)
		ext := schema.ExtensionName("postgis")
		sql, err := ext.Select(bind, pg.Delete)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("CASCADE", bind.Get("cascade"))
	})

	t.Run("EmptyName", func(t *testing.T) {
		bind := pg.NewBind()
		ext := schema.ExtensionName("")
		_, err := ext.Select(bind, pg.Get)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("WhitespaceOnlyName", func(t *testing.T) {
		bind := pg.NewBind()
		ext := schema.ExtensionName("   ")
		_, err := ext.Select(bind, pg.Get)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		ext := schema.ExtensionName("postgis")
		_, err := ext.Select(bind, pg.Insert)
		assert.Error(err)
	})
}

func Test_ExtensionMeta_Insert(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidInsert", func(t *testing.T) {
		bind := pg.NewBind()
		meta := schema.ExtensionMeta{Name: "postgis"}
		sql, err := meta.Insert(bind)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "CREATE EXTENSION")
		assert.Equal("postgis", bind.Get("name")) // Raw name - template handles quoting
		assert.Equal("", bind.Get("with"))
		assert.Equal("", bind.Get("version"))
	})

	t.Run("InsertWithSchema", func(t *testing.T) {
		bind := pg.NewBind()
		meta := schema.ExtensionMeta{Name: "postgis", Schema: "myschema"}
		_, err := meta.Insert(bind)
		assert.NoError(err)
		with := bind.Get("with").(string)
		assert.Contains(with, "WITH SCHEMA")
		assert.Contains(with, "myschema")
	})

	t.Run("InsertWithVersion", func(t *testing.T) {
		bind := pg.NewBind()
		meta := schema.ExtensionMeta{Name: "postgis", Version: "3.3.0"}
		_, err := meta.Insert(bind)
		assert.NoError(err)
		version := bind.Get("version").(string)
		assert.Contains(version, "VERSION")
		assert.Contains(version, "3.3.0")
	})

	t.Run("InsertWithSchemaAndVersion", func(t *testing.T) {
		bind := pg.NewBind()
		meta := schema.ExtensionMeta{Name: "postgis", Schema: "public", Version: "3.3.0"}
		sql, err := meta.Insert(bind)
		assert.NoError(err)
		assert.NotEmpty(sql)
		with := bind.Get("with").(string)
		assert.Contains(with, "WITH SCHEMA")
		version := bind.Get("version").(string)
		assert.Contains(version, "VERSION")
	})

	t.Run("EmptyName", func(t *testing.T) {
		bind := pg.NewBind()
		meta := schema.ExtensionMeta{Name: ""}
		_, err := meta.Insert(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("WhitespaceOnlyName", func(t *testing.T) {
		bind := pg.NewBind()
		meta := schema.ExtensionMeta{Name: "   "}
		_, err := meta.Insert(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("InsertDefaultNoCascade", func(t *testing.T) {
		bind := pg.NewBind()
		meta := schema.ExtensionMeta{Name: "postgis"}
		_, err := meta.Insert(bind)
		assert.NoError(err)
		assert.Equal("", bind.Get("cascade"))
	})

	t.Run("InsertWithCascade", func(t *testing.T) {
		bind := pg.NewBind()
		bind.Set("cascade", true)
		meta := schema.ExtensionMeta{Name: "postgis"}
		_, err := meta.Insert(bind)
		assert.NoError(err)
		assert.Equal("CASCADE", bind.Get("cascade"))
	})
}

func Test_ExtensionMeta_Update(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidUpdate", func(t *testing.T) {
		bind := pg.NewBind()
		meta := schema.ExtensionMeta{Name: "postgis"}
		err := meta.Update(bind)
		assert.NoError(err)
		assert.Equal("postgis", bind.Get("name")) // Raw name - template handles quoting
		assert.Equal("", bind.Get("version"))
	})

	t.Run("UpdateWithVersion", func(t *testing.T) {
		bind := pg.NewBind()
		meta := schema.ExtensionMeta{Name: "postgis", Version: "3.4.0"}
		err := meta.Update(bind)
		assert.NoError(err)
		version := bind.Get("version").(string)
		assert.Contains(version, "TO")
		assert.Contains(version, "3.4.0")
	})

	t.Run("EmptyName", func(t *testing.T) {
		bind := pg.NewBind()
		meta := schema.ExtensionMeta{Name: ""}
		err := meta.Update(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("WhitespaceOnlyName", func(t *testing.T) {
		bind := pg.NewBind()
		meta := schema.ExtensionMeta{Name: "   "}
		err := meta.Update(bind)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})
}

func Test_ExtensionName_SpecialCharacters(t *testing.T) {
	assert := assert.New(t)

	t.Run("NameWithQuotes", func(t *testing.T) {
		bind := pg.NewBind()
		ext := schema.ExtensionName("my\"ext")
		sql, err := ext.Select(bind, pg.Delete)
		assert.NoError(err)
		assert.NotEmpty(sql)
		// Raw name stored - template handles escaping
		name := bind.Get("name").(string)
		assert.Equal("my\"ext", name)
	})

	t.Run("NameWithSpaces", func(t *testing.T) {
		bind := pg.NewBind()
		ext := schema.ExtensionName("my extension")
		sql, err := ext.Select(bind, pg.Delete)
		assert.NoError(err)
		assert.NotEmpty(sql)
		// Raw name stored - template handles quoting
		name := bind.Get("name").(string)
		assert.Equal("my extension", name)
	})
}
