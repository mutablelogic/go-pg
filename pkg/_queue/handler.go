package queue

import (
	"context"
	"encoding/json"

	// Packages
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// TaskHandler processes a task. Return nil on success, or an error to fail the task.
type TaskHandler func(context.Context, *schema.Task) error

// TickerHandler processes a ticker. Return nil on success, or an error on failure.
type TickerHandler func(context.Context, *schema.Ticker) error

// Worker processes a task or ticker payload.
// Return nil on success, or an error to indicate failure.
type Worker interface {
	Run(ctx context.Context, payload json.RawMessage) error
}
