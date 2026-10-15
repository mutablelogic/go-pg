package schema_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
)

func TestTickerTable(t *testing.T) {
	lastAt := time.Date(2026, 10, 15, 9, 30, 0, 0, time.Local)
	ticker := schema.Ticker{Ticker: "hourly", TickerMeta: schema.TickerMeta{
		Payload:  json.RawMessage(`{ "kind" : "hourly" }`),
		Interval: types.Ptr(time.Hour),
	}, LastAt: &lastAt}
	assert.Equal(t, []any{"hourly", "1h0m0s", "2026-10-15 09:30:00", `{"kind":"hourly"}`}, cellsOf(ticker.Header(), ticker.Cell))

	// A ticker which hasn't run has no last run
	assert.Equal(t, "", schema.Ticker{Ticker: "new"}.Cell(2))
}

func TestTaskTable(t *testing.T) {
	createdAt := time.Date(2026, 10, 15, 9, 30, 0, 0, time.Local)
	task := schema.TaskWithStatus{Task: schema.Task{
		Id:        42,
		Queue:     "queue",
		Worker:    types.Ptr("worker"),
		Result:    json.RawMessage(`"boom"`),
		TaskMeta:  schema.TaskMeta{Payload: json.RawMessage(`{"path":"` + strings.Repeat("a", 50) + `"}`)},
		CreatedAt: &createdAt,
		Retries:   types.Ptr(uint64(2)),
	}, Status: "retry"}
	row := cellsOf(task.Header(), task.Cell)
	assert.Equal(t, []any{"42", "queue", "retry", "2", "worker", "2026-10-15 09:30:00"}, row[:6])

	// A long payload is shortened, and a null result is empty
	assert.Len(t, []rune(row[6].(string)), 40)
	assert.True(t, strings.HasSuffix(row[6].(string), "…"))
	assert.Equal(t, `"boom"`, row[7])
	task.Result = json.RawMessage(`null`)
	assert.Equal(t, "", task.Cell(7))
}

// cellsOf returns the cells of a table row, given its header and cells
func cellsOf[T any](header []string, cell func(int) T) []any {
	result := make([]any, len(header))
	for i := range result {
		result[i] = cell(i)
	}
	return result
}

func TestListQuery(t *testing.T) {
	// The offset, limit and status are query parameters, when they're set
	assert.Empty(t, schema.TickerListRequest{}.Query())
	assert.Equal(t, "limit=10&offset=5", schema.TickerListRequest{OffsetLimit: pg.OffsetLimit{Offset: 5, Limit: types.Ptr(uint64(10))}}.Query().Encode())
	assert.Equal(t, "status=failed", schema.TaskListRequest{Status: " failed "}.Query().Encode())
	assert.Equal(t, "queue=one&queue=two", schema.TaskListRequest{Queue: []string{"one", " ", "two"}}.Query().Encode())
	assert.Equal(t, "offset=1", schema.QueueListRequest{OffsetLimit: pg.OffsetLimit{Offset: 1}}.Query().Encode())
}
