package schema

import (
	"context"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// ContextKey is the type of the keys of the values in the context of a task
// or ticker callback
type ContextKey int

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// ContextKeyTask is the key of the task, a *Task, in the context of a
	// queue's task callback
	ContextKeyTask ContextKey = iota

	// ContextKeyTicker is the key of the ticker, a *Ticker, in the context of
	// a ticker's callback
	ContextKeyTicker
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// TaskFromContext returns the task in the context of a queue's task callback,
// or nil if there isn't one
func TaskFromContext(ctx context.Context) *Task {
	task, _ := ctx.Value(ContextKeyTask).(*Task)
	return task
}

// TickerFromContext returns the ticker in the context of a ticker's callback,
// or nil if there isn't one
func TickerFromContext(ctx context.Context) *Ticker {
	ticker, _ := ctx.Value(ContextKeyTicker).(*Ticker)
	return ticker
}
