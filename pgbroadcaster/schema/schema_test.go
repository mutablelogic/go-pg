package schema_test

import (
	"encoding/json"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgbroadcaster/schema"
	assert "github.com/stretchr/testify/assert"
)

func TestEventFilterMatches(t *testing.T) {
	// A filter matches the events with its schema, table and action, each of
	// which matches any when it's empty
	event := schema.Event{Schema: "filer", Table: "volume", Action: "UPDATE", Row: "42"}
	for _, test := range []struct {
		filter schema.EventFilter
		want   bool
	}{
		{schema.EventFilter{}, true},
		{schema.EventFilter{Schema: "filer"}, true},
		{schema.EventFilter{Schema: "filer", Table: "volume"}, true},
		{schema.EventFilter{Schema: "filer", Table: "volume", Action: "UPDATE"}, true},
		{schema.EventFilter{Action: "UPDATE"}, true},
		{schema.EventFilter{Schema: "llm"}, false},
		{schema.EventFilter{Schema: "filer", Table: "object"}, false},
		{schema.EventFilter{Table: "volume", Action: "DELETE"}, false},
	} {
		assert.Equal(t, test.want, test.filter.Matches(event), test.filter)
	}
}

func TestEventFilterQuery(t *testing.T) {
	assert.Equal(t, "", schema.EventFilter{}.Query().Encode())
	assert.Equal(t, "action=INSERT&schema=filer&table=volume", schema.EventFilter{Schema: "filer", Table: "volume", Action: "INSERT"}.Query().Encode())
}

func TestEventRow(t *testing.T) {
	// The row is optional, so events without one are as they were
	assert.JSONEq(t, `{"schema":"filer","table":"volume","action":"UPDATE"}`, mustJSON(t, schema.Event{Schema: "filer", Table: "volume", Action: "UPDATE"}))
	assert.JSONEq(t, `{"schema":"filer","table":"volume","action":"UPDATE","row":"42"}`, mustJSON(t, schema.Event{Schema: "filer", Table: "volume", Action: "UPDATE", Row: "42"}))
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
