package pg

import (
	"testing"

	pgx "github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func Test_args_001(t *testing.T) {
	assert := assert.New(t)

	assert.Nil(args("", nil))
	assert.Equal([]any{"value"}, args("", []any{"value"}))
	assert.Equal([]any{"value", 42}, args("", []any{"value", 42}))
}

func Test_args_002(t *testing.T) {
	assert := assert.New(t)

	actual := args(
		"UPDATE test SET name=@name WHERE id=@id RETURNING id, name",
		[]any{pgx.NamedArgs{
			"id":       101,
			"name":     "alice",
			"unused":   true,
			"otelspan": "test.query",
		}},
	)

	assert.Equal(pgx.NamedArgs{
		"id":   101,
		"name": "alice",
	}, actual)
}

func Test_args_003(t *testing.T) {
	assert := assert.New(t)

	actual := args(
		"SELECT * FROM public.users WHERE id=@id AND name IN ('alice','bob')",
		[]any{pgx.NamedArgs{
			"schema":   "public",
			"id":       22,
			"names":    []string{"alice", "bob"},
			"unused":   true,
			"otelspan": "test.query",
		}},
	)

	assert.Equal(pgx.NamedArgs{
		"id": 22,
	}, actual)
}

func Test_args_004(t *testing.T) {
	assert := assert.New(t)

	actual := args(
		"SELECT 1",
		[]any{pgx.NamedArgs{
			"unused":   true,
			"otelspan": "test.query",
		}},
	)

	assert.Nil(actual)
}

func Test_args_005(t *testing.T) {
	assert := assert.New(t)

	actual := args(
		"UPDATE test SET name=@name WHERE id=@id RETURNING id, name",
		[]any{map[string]any{
			"id":       101,
			"name":     "alice",
			"unused":   true,
			"otelspan": "test.query",
		}},
	)

	assert.Equal(pgx.NamedArgs{
		"id":   101,
		"name": "alice",
	}, actual)
}

func Test_spanName(t *testing.T) {
	for _, test := range []struct {
		sql  string
		args []any
		want string
	}{
		// A query with a name
		{"SELECT 1", []any{pgx.NamedArgs{"otelspan": "test.query"}}, "test.query"},
		{"SELECT 1", []any{map[string]any{"otelspan": "test.query"}}, "test.query"},
		{"COMMIT", []any{pgx.NamedArgs{"otelspan": "test.query"}}, "test.query"},

		// A transaction's statements, as pgx sends them
		{"begin", nil, "pg.transaction"},
		{"begin isolation level serializable read only", nil, "pg.transaction"},
		{"START TRANSACTION", nil, "pg.transaction"},
		{"savepoint sp_1", nil, "pg.transaction"},
		{"commit", nil, "pg.commit"},
		{"release savepoint sp_1", nil, "pg.commit"},
		{"rollback", nil, "pg.rollback"},
		{"rollback to savepoint sp_1", nil, "pg.rollback"},
		{"  ROLLBACK  ", nil, "pg.rollback"},

		// Statements ended with a semicolon
		{"BEGIN;", nil, "pg.transaction"},
		{"START TRANSACTION;", nil, "pg.transaction"},
		{"savepoint sp_1;", nil, "pg.transaction"},
		{"COMMIT;", nil, "pg.commit"},
		{"END;", nil, "pg.commit"},
		{"ROLLBACK;", nil, "pg.rollback"},
		{"ROLLBACK ;", nil, "pg.rollback"},
		{"rollback to savepoint sp_1;", nil, "pg.rollback"},
		{";", nil, "pg.query"},
		{"SELECT 1;", nil, "pg.query"},

		// Other queries
		{"SELECT 1", nil, "pg.query"},
		{"SELECT 1", []any{pgx.NamedArgs{"otelspan": ""}}, "pg.query"},
		{"SELECT 1", []any{pgx.NamedArgs{"id": 1}}, "pg.query"},
		{"START something", nil, "pg.query"},
		{"", nil, "pg.query"},
	} {
		assert.Equal(t, test.want, spanName(test.sql, test.args), test.sql)
	}
}
