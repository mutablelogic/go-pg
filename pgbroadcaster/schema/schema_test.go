package schema_test

import (
	"encoding/json"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgbroadcaster/schema"
	changes "github.com/mutablelogic/go-server/broadcaster/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestNotificationChange(t *testing.T) {
	// A trigger's action, which is its TG_OP, and a published change's, are
	// the change's action, with its schema, table, row and data
	for action, want := range map[string]changes.ChangeAction{
		"INSERT": changes.ChangeCreate,
		"UPDATE": changes.ChangeUpdate,
		"DELETE": changes.ChangeDelete,
		"create": changes.ChangeCreate,
		"update": changes.ChangeUpdate,
		"delete": changes.ChangeDelete,
	} {
		change, err := schema.Notification{Schema: "filer", Table: "volume", Action: action, Row: "42", Data: json.RawMessage(`{"serial":1}`)}.Change()
		require.NoError(t, err, action)
		assert.Equal(t, changes.Change{Action: want, Schema: "filer", Table: "volume", Row: "42", Data: json.RawMessage(`{"serial":1}`)}, change, action)
	}

	// Another action isn't a change
	for _, action := range []string{"TRUNCATE", ""} {
		_, err := schema.Notification{Table: "volume", Action: action}.Change()
		assert.Error(t, err, action)
	}
}

func TestNotificationJSON(t *testing.T) {
	// A trigger's notification is decoded, and a published change is a
	// notification
	var n schema.Notification
	require.NoError(t, json.Unmarshal([]byte(`{"schema":"feed","table":"item","action":"UPDATE","row":"{\"id\":\"1\",\"feed\":2}"}`), &n))
	assert.Equal(t, schema.Notification{Schema: "feed", Table: "item", Action: "UPDATE", Row: `{"id":"1","feed":2}`}, n)
	data, err := json.Marshal(changes.Change{Action: changes.ChangeDelete, Table: "item"})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &n))
	change, err := n.Change()
	require.NoError(t, err)
	assert.Equal(t, changes.ChangeDelete, change.Action)
}
