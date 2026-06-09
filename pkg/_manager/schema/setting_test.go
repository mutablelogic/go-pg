package schema_test

import (
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_SettingListRequest_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("ListOperation", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.SettingListRequest{}
		q, err := r.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(q)
		assert.Contains(q, "pg_catalog.pg_settings")
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.SettingListRequest{}
		_, err := r.Select(bind, pg.Get)
		assert.Error(err)
	})

	t.Run("WithCategory", func(t *testing.T) {
		bind := pg.NewBind()
		category := "Connections and Authentication"
		r := schema.SettingListRequest{Category: &category}
		q, err := r.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(q)
		where := bind.Get("where").(string)
		assert.Contains(where, "category")
		assert.Equal(category, bind.Get("category"))
	})

	t.Run("WithoutCategory", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.SettingListRequest{}
		_, err := r.Select(bind, pg.List)
		assert.NoError(err)
		assert.Equal("", bind.Get("where"))
	})

	t.Run("Ordering", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.SettingListRequest{}
		_, err := r.Select(bind, pg.List)
		assert.NoError(err)
		orderby := bind.Get("orderby").(string)
		assert.Contains(orderby, "category")
		assert.Contains(orderby, "name")
	})

	t.Run("OffsetLimit", func(t *testing.T) {
		bind := pg.NewBind()
		limit := uint64(50)
		r := schema.SettingListRequest{
			OffsetLimit: pg.OffsetLimit{Offset: 10, Limit: &limit},
		}
		_, err := r.Select(bind, pg.List)
		assert.NoError(err)
		offsetlimit := bind.Get("offsetlimit").(string)
		assert.Contains(offsetlimit, "LIMIT 50")
		assert.Contains(offsetlimit, "OFFSET 10")
	})

	t.Run("DefaultLimit", func(t *testing.T) {
		bind := pg.NewBind()
		r := schema.SettingListRequest{}
		_, err := r.Select(bind, pg.List)
		assert.NoError(err)
		offsetlimit := bind.Get("offsetlimit").(string)
		// Default limit should be SettingListLimit (500)
		assert.Contains(offsetlimit, "LIMIT")
	})
}

func Test_Setting_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("WithUnit", func(t *testing.T) {
		unit := "kB"
		value := "131072"
		s := schema.Setting{
			Name:        "shared_buffers",
			SettingMeta: schema.SettingMeta{Value: &value},
			Unit:        &unit,
			Category:    "Resource Usage / Memory",
			Description: "Sets the number of shared memory buffers",
		}
		str := s.String()
		assert.Contains(str, "shared_buffers")
		assert.Contains(str, "131072")
		assert.Contains(str, "kB")
	})

	t.Run("WithoutUnit", func(t *testing.T) {
		value := "100"
		s := schema.Setting{
			Name:        "max_connections",
			SettingMeta: schema.SettingMeta{Value: &value},
			Category:    "Connections and Authentication",
			Description: "Sets the maximum number of concurrent connections",
		}
		str := s.String()
		assert.Contains(str, "max_connections")
		assert.Contains(str, "100")
		assert.NotContains(str, "unit")
	})
}

func Test_SettingList_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("EmptyList", func(t *testing.T) {
		list := schema.SettingList{
			Count: 0,
			Body:  []schema.Setting{},
		}
		str := list.String()
		assert.Contains(str, "count")
		assert.Contains(str, "0")
	})

	t.Run("WithSettings", func(t *testing.T) {
		value1 := "100"
		value2 := "128MB"
		list := schema.SettingList{
			Count: 2,
			Body: []schema.Setting{
				{Name: "max_connections", SettingMeta: schema.SettingMeta{Value: &value1}, Category: "Connections"},
				{Name: "shared_buffers", SettingMeta: schema.SettingMeta{Value: &value2}, Category: "Memory"},
			},
		}
		str := list.String()
		assert.Contains(str, "max_connections")
		assert.Contains(str, "shared_buffers")
	})
}
