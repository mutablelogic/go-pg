package schema_test

import (
	"context"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	assert "github.com/stretchr/testify/assert"
)

func TestContext(t *testing.T) {
	// Without a task or ticker, there's none
	assert.Nil(t, schema.TaskFromContext(context.Background()))
	assert.Nil(t, schema.TickerFromContext(context.Background()))

	// With them, by their keys
	task := &schema.Task{Id: 1, Queue: "queue"}
	ticker := &schema.Ticker{Ticker: "ticker"}
	ctx := context.WithValue(context.Background(), schema.ContextKeyTask, task)
	ctx = context.WithValue(ctx, schema.ContextKeyTicker, ticker)
	assert.Same(t, task, schema.TaskFromContext(ctx))
	assert.Same(t, ticker, schema.TickerFromContext(ctx))

	// A value of another type isn't returned
	ctx = context.WithValue(context.Background(), schema.ContextKeyTask, "not a task")
	assert.Nil(t, schema.TaskFromContext(ctx))
}
