package pg

import (
	"testing"

	pgx "github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func Test_args_001(t *testing.T) {
	assert := assert.New(t)

	assert.Nil(args("", nil))
	assert.Equal("value", args("", []any{"value"}))
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
