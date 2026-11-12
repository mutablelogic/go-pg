package schema

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"

	// Packages
	changes "github.com/mutablelogic/go-server/broadcaster/schema"
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

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed queries.sql
var Queries string

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Notification is what's notified on the broadcaster's channel: by the
// database's triggers, whose action is INSERT, UPDATE or DELETE, as their
// TG_OP, or by Publish, whose action is create, update or delete. It says
// what changed, which is a table of a schema, and which row, or rows,
// changed, when it says, and what the table says of the change, when it
// says anything.
type Notification struct {
	Schema string          `json:"schema,omitempty"`
	Table  string          `json:"table,omitempty"`
	Action string          `json:"action,omitempty"`
	Row    string          `json:"row,omitempty"`
	Data   json.RawMessage `json:"data,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Change returns the notification as a change, whose action is create,
// update or delete, or an error when its action is another, such as
// TRUNCATE
func (n Notification) Change() (changes.Change, error) {
	var action changes.ChangeAction
	switch strings.ToUpper(n.Action) {
	case "INSERT", "CREATE":
		action = changes.ChangeCreate
	case "UPDATE":
		action = changes.ChangeUpdate
	case "DELETE":
		action = changes.ChangeDelete
	default:
		return changes.Change{}, fmt.Errorf("unknown action: %q", n.Action)
	}
	return changes.Change{Action: action, Schema: n.Schema, Table: n.Table, Row: n.Row, Data: n.Data}, nil
}
