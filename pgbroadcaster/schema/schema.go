package schema

import (
	_ "embed"
	"net/url"
)

////////////////////////////////////////////////////////////////////////////////
// CONSTANTS

const (
	// DefaultSchema is the database schema when it isn't set
	DefaultSchema = "broadcaster"

	// NotifyChannel is the channel the broadcaster listens on, by default,
	// which the modules notify of their changes on
	NotifyChannel = "events"
)

const (
	// MaxPayloadSize is the largest event, as JSON, which can be published,
	// which is the largest notification Postgres sends, less a byte
	MaxPayloadSize = 7999
)

const (
	// EventName is the name of each server-sent event which is streamed to
	// a client watching events
	EventName = "event"

	// EventShutdown is the name of the server-sent event which is streamed
	// to a client watching events when the server stops, before the stream
	// ends, so the client knows it ended as there will be no more events
	EventShutdown = "shutdown"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed queries.sql
var Queries string

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Event is a change which is published to the broadcaster's channel: what
// changed, such as a table of a schema, how, such as INSERT, UPDATE or
// DELETE, as the database's triggers notify of their tables' changes, and
// which row, or rows, changed, when it says, such as by an identifier which
// the rows share
type Event struct {
	Schema string `json:"schema,omitempty"`
	Table  string `json:"table,omitempty"`
	Action string `json:"action,omitempty"`
	Row    string `json:"row,omitempty"`
}

// EventFilter matches events: those with its schema, table and action, each
// of which matches any when it's empty
type EventFilter struct {
	Schema string `json:"schema,omitempty" help:"returns only events of the schema"`
	Table  string `json:"table,omitempty" help:"returns only events of the table"`
	Action string `json:"action,omitempty" help:"returns only events of the action, such as INSERT, UPDATE or DELETE"`
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Matches returns true when the event has the filter's schema, table and
// action, each of which matches any when it's empty
func (f EventFilter) Matches(event Event) bool {
	return (f.Schema == "" || f.Schema == event.Schema) &&
		(f.Table == "" || f.Table == event.Table) &&
		(f.Action == "" || f.Action == event.Action)
}

// Query returns the filter as the query of a request to watch events
func (f EventFilter) Query() url.Values {
	q := url.Values{}
	if f.Schema != "" {
		q.Set("schema", f.Schema)
	}
	if f.Table != "" {
		q.Set("table", f.Table)
	}
	if f.Action != "" {
		q.Set("action", f.Action)
	}
	return q
}
