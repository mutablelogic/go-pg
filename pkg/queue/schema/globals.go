package schema

import (
	"encoding/json"
	"time"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	SchemaName        = "pgqueue"
	DefaultNamespace  = "default"
	QueueListLimit    = 100
	TickerListLimit   = 100
	TickerPeriod      = 15 * time.Second
	TaskPeriod        = 15 * time.Second
	CleanupTickerName = "cleanup"
	CleanupInterval   = 1 * time.Hour
	TopicQueueInsert  = "_queue_insert" // pg_notify topic suffix for task inserts
)

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func stringify[T any](v T) string {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}
