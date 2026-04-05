package broadcaster

import (
	"github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// ChangeNotification is emitted for table changes when PostgreSQL NOTIFY
// payloads match the broadcaster JSON schema.
type ChangeNotification struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	Action string `json:"action"`
}

///////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (n ChangeNotification) String() string {
	return types.Stringify(n)
}

///////////////////////////////////////////////////////////////////////////////
// MATCHES

// Matches returns true if the notification matches the given schema, table, and action
// filters. Empty filters match all values.
func (n ChangeNotification) Matches(schema, table, action string) bool {
	if schema != "" && n.Schema != schema {
		return false
	}
	if table != "" && n.Table != table {
		return false
	}
	if action != "" && n.Action != action {
		return false
	}
	return true
}
