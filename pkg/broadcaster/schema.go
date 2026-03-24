package broadcaster

import "encoding/json"

///////////////////////////////////////////////////////////////////////////////
// TYPES

// ChangeNotification is emitted for table changes when PostgreSQL NOTIFY
// payloads match the broadcaster JSON schema.
type ChangeNotification struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	Action string `json:"action"`
}

func (n ChangeNotification) String() string {
	data, err := json.MarshalIndent(n, "", "  ")
	if err != nil {
		return "{}"
	}
	return string(data)
}
