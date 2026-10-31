package schema_test

import (
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgbroadcaster/schema"
	assert "github.com/stretchr/testify/assert"
)

func TestEventFilterMatches(t *testing.T) {
	// A filter matches the events with its schema, table and action, each of
	// which matches any when it's empty
	event := schema.Event{Schema: "filer", Table: "volume", Action: "UPDATE"}
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
