package pg

import (
	"testing"

	// Packages
	pgx "github.com/jackc/pgx/v5"
	pgconn "github.com/jackc/pgx/v5/pgconn"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_NormalizeError_001(t *testing.T) {
	assert := assert.New(t)

	err := NormalizeError(pgx.ErrNoRows)
	assert.ErrorIs(err, ErrNotFound)
	assert.False(IsDatabaseError(err))
	assert.Empty(SQLState(err))
}

func Test_NormalizeError_002(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	err := NormalizeError(&pgconn.PgError{Code: sqlStateUniqueViolation, Message: "duplicate key value violates unique constraint"})
	assert.ErrorIs(err, ErrDatabase)
	assert.ErrorIs(err, ErrConflict)
	assert.ErrorIs(err, ErrUniqueViolation)
	assert.NotErrorIs(err, ErrBadParameter)
	assert.True(IsDatabaseError(err))
	assert.Equal(sqlStateUniqueViolation, SQLState(err))

	var dbErr *DatabaseError
	require.ErrorAs(err, &dbErr)
	assert.Equal(sqlStateUniqueViolation, dbErr.SQLState())
	assert.Equal("duplicate key value violates unique constraint", dbErr.Message())
	assert.ErrorAs(err, new(*pgconn.PgError))
}

func Test_NormalizeError_003(t *testing.T) {
	tests := []struct {
		name string
		code string
		kind Err
	}{
		{"foreign_key", sqlStateForeignKeyViolation, ErrForeignKeyViolation},
		{"not_null", sqlStateNotNullViolation, ErrNotNullViolation},
		{"check", sqlStateCheckViolation, ErrCheckViolation},
		{"invalid_text", sqlStateInvalidTextRepresentation, ErrInvalidTextRepresentation},
		{"invalid_datetime", sqlStateInvalidDatetimeFormat, ErrInvalidDatetimeFormat},
		{"datetime_overflow", sqlStateDatetimeFieldOverflow, ErrDatetimeFieldOverflow},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)

			err := NormalizeError(&pgconn.PgError{Code: tc.code, Message: tc.name})
			assert.ErrorIs(err, ErrDatabase)
			assert.ErrorIs(err, ErrBadParameter)
			assert.ErrorIs(err, tc.kind)
			assert.Equal(tc.code, SQLState(err))
		})
		}
	}