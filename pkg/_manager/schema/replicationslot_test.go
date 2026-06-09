package schema_test

import (
	"encoding/json"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_ReplicationSlot_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidSlot", func(t *testing.T) {
		lagBytes := int64(1024)
		lagMs := 0.5
		s := schema.ReplicationSlot{
			ReplicationSlotMeta: schema.ReplicationSlotMeta{
				Name:     "my_slot",
				Type:     "physical",
				Database: "",
			},
			Status:     "streaming",
			ClientAddr: "192.168.1.100",
			LagBytes:   &lagBytes,
			LagMs:      &lagMs,
		}
		str := s.String()
		assert.NotEmpty(str)

		var parsed schema.ReplicationSlot
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal(s.Name, parsed.Name)
		assert.Equal(s.Status, parsed.Status)
	})

	t.Run("EmptySlot", func(t *testing.T) {
		s := schema.ReplicationSlot{}
		str := s.String()
		assert.NotEmpty(str)

		var parsed schema.ReplicationSlot
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
	})
}

func Test_ReplicationSlotMeta_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("PhysicalSlot", func(t *testing.T) {
		m := schema.ReplicationSlotMeta{
			Name: "my_slot",
			Type: "physical",
		}
		str := m.String()
		assert.NotEmpty(str)

		var parsed schema.ReplicationSlotMeta
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal("my_slot", parsed.Name)
	})

	t.Run("LogicalSlot", func(t *testing.T) {
		m := schema.ReplicationSlotMeta{
			Name:     "my_logical_slot",
			Type:     "logical",
			Plugin:   "pgoutput",
			Database: "mydb",
		}
		str := m.String()
		assert.NotEmpty(str)

		var parsed schema.ReplicationSlotMeta
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal("pgoutput", parsed.Plugin)
	})
}

func Test_ReplicationSlotList_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("WithSlots", func(t *testing.T) {
		l := schema.ReplicationSlotList{
			Count: 2,
			Body: []schema.ReplicationSlot{
				{ReplicationSlotMeta: schema.ReplicationSlotMeta{Name: "slot1", Type: "physical"}, Status: "inactive"},
				{ReplicationSlotMeta: schema.ReplicationSlotMeta{Name: "slot2", Type: "logical"}, Status: "streaming"},
			},
		}
		str := l.String()
		assert.NotEmpty(str)

		var parsed schema.ReplicationSlotList
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal(uint64(2), parsed.Count)
		assert.Len(parsed.Body, 2)
	})

	t.Run("EmptyList", func(t *testing.T) {
		l := schema.ReplicationSlotList{}
		str := l.String()
		assert.NotEmpty(str)

		var parsed schema.ReplicationSlotList
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
	})
}

func Test_ReplicationSlotMeta_Validate(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidPhysical", func(t *testing.T) {
		m := schema.ReplicationSlotMeta{Name: "my_slot", Type: "physical"}
		err := m.Validate()
		assert.NoError(err)
	})

	t.Run("ValidLogical", func(t *testing.T) {
		m := schema.ReplicationSlotMeta{Name: "my_slot", Type: "logical", Plugin: "pgoutput"}
		err := m.Validate()
		assert.NoError(err)
	})

	t.Run("EmptyName", func(t *testing.T) {
		m := schema.ReplicationSlotMeta{Name: "", Type: "physical"}
		err := m.Validate()
		assert.Error(err)
	})

	t.Run("WhitespaceOnlyName", func(t *testing.T) {
		m := schema.ReplicationSlotMeta{Name: "   ", Type: "physical"}
		err := m.Validate()
		assert.Error(err)
	})

	t.Run("ReservedPrefixName", func(t *testing.T) {
		m := schema.ReplicationSlotMeta{Name: "pg_my_slot", Type: "physical"}
		err := m.Validate()
		assert.Error(err)
	})

	t.Run("InvalidType", func(t *testing.T) {
		m := schema.ReplicationSlotMeta{Name: "my_slot", Type: "invalid"}
		err := m.Validate()
		assert.Error(err)
	})

	t.Run("EmptyType", func(t *testing.T) {
		m := schema.ReplicationSlotMeta{Name: "my_slot", Type: ""}
		err := m.Validate()
		assert.Error(err)
	})

	t.Run("LogicalWithoutPlugin", func(t *testing.T) {
		m := schema.ReplicationSlotMeta{Name: "my_slot", Type: "logical"}
		err := m.Validate()
		assert.Error(err)
	})

	t.Run("LogicalWithEmptyPlugin", func(t *testing.T) {
		m := schema.ReplicationSlotMeta{Name: "my_slot", Type: "logical", Plugin: "   "}
		err := m.Validate()
		assert.Error(err)
	})

	t.Run("TypeCaseInsensitive", func(t *testing.T) {
		m := schema.ReplicationSlotMeta{Name: "my_slot", Type: "PHYSICAL"}
		err := m.Validate()
		assert.NoError(err)
	})
}

func Test_ReplicationSlotName_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("GetOperation", func(t *testing.T) {
		bind := pg.NewBind()
		n := schema.ReplicationSlotName("my_slot")
		sql, err := n.Select(bind, pg.Get)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("my_slot", bind.Get("name"))
	})

	t.Run("DeleteOperation", func(t *testing.T) {
		bind := pg.NewBind()
		n := schema.ReplicationSlotName("my_slot")
		sql, err := n.Select(bind, pg.Delete)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "pg_drop_replication_slot")
	})

	t.Run("EmptyName", func(t *testing.T) {
		bind := pg.NewBind()
		n := schema.ReplicationSlotName("")
		_, err := n.Select(bind, pg.Get)
		assert.Error(err)
	})

	t.Run("ReservedPrefix", func(t *testing.T) {
		bind := pg.NewBind()
		n := schema.ReplicationSlotName("pg_my_slot")
		_, err := n.Select(bind, pg.Get)
		assert.Error(err)
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		n := schema.ReplicationSlotName("my_slot")
		_, err := n.Select(bind, pg.Update)
		assert.Error(err)
	})
}

func Test_ReplicationSlotListRequest_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("ListOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.ReplicationSlotListRequest{}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("ORDER BY name ASC", bind.Get("orderby"))
		assert.Equal("", bind.Get("where"))
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.ReplicationSlotListRequest{}
		_, err := req.Select(bind, pg.Get)
		assert.Error(err)
	})
}

func Test_ReplicationSlotMeta_Insert(t *testing.T) {
	assert := assert.New(t)

	t.Run("CreatePhysical", func(t *testing.T) {
		bind := pg.NewBind()
		m := schema.ReplicationSlotMeta{Name: "my_slot", Type: "physical"}
		sql, err := m.Insert(bind)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "pg_create_physical_replication_slot")
		assert.Equal("my_slot", bind.Get("name"))
	})

	t.Run("CreateLogical", func(t *testing.T) {
		bind := pg.NewBind()
		m := schema.ReplicationSlotMeta{Name: "my_slot", Type: "logical", Plugin: "pgoutput"}
		sql, err := m.Insert(bind)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Contains(sql, "pg_create_logical_replication_slot")
		assert.Equal("pgoutput", bind.Get("plugin"))
	})

	t.Run("CreateWithTemporary", func(t *testing.T) {
		bind := pg.NewBind()
		m := schema.ReplicationSlotMeta{Name: "my_slot", Type: "physical", Temporary: true}
		_, err := m.Insert(bind)
		assert.NoError(err)
		assert.Equal(true, bind.Get("temporary"))
	})

	t.Run("CreateWithTwoPhase", func(t *testing.T) {
		bind := pg.NewBind()
		m := schema.ReplicationSlotMeta{Name: "my_slot", Type: "logical", Plugin: "pgoutput", TwoPhase: true}
		_, err := m.Insert(bind)
		assert.NoError(err)
		assert.Equal(true, bind.Get("two_phase"))
	})

	t.Run("InvalidMeta", func(t *testing.T) {
		bind := pg.NewBind()
		m := schema.ReplicationSlotMeta{Name: "", Type: "physical"}
		_, err := m.Insert(bind)
		assert.Error(err)
	})
}
