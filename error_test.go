package pg

import (
	"errors"
	"testing"

	// Packages
	pgx "github.com/jackc/pgx/v5"
	pgconn "github.com/jackc/pgx/v5/pgconn"
	"github.com/mutablelogic/go-server/pkg/httpresponse"
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

func Test_HTTPError_001(t *testing.T) {
	assert := assert.New(t)

	err := HTTPError(&pgconn.PgError{Code: sqlStateDependentObjectsStillExist, Message: "cannot drop schema public because other objects depend on it"})
	assert.ErrorIs(err, httpresponse.ErrConflict)
}

func Test_HTTPError_002(t *testing.T) {
	assert := assert.New(t)

	err := HTTPError(&pgconn.PgError{Code: sqlStateInsufficientPrivilege, Message: "could not set permissions on directory \"/media\": Operation not permitted"})
	assert.ErrorIs(err, httpresponse.ErrBadRequest)
}

func Test_HTTPError_003(t *testing.T) {
	assert := assert.New(t)

	err := HTTPError(&pgconn.PgError{Code: sqlStateUndefinedFile, Message: "directory \"/tmp/test\" does not exist"})
	assert.ErrorIs(err, httpresponse.ErrNotFound)
}

func Test_Err_Withf(t *testing.T) {
	assert := assert.New(t)
	cause := errors.New("cause")

	// The error is the code, and wraps an error with %w
	err := ErrBadParameter.Withf("invalid %q: %w", "name", cause)
	assert.ErrorIs(err, ErrBadParameter)
	assert.ErrorIs(err, cause)
	assert.Equal(`bad parameter: invalid "name": cause`, err.Error())

	// Without %w, the message is the same, and nothing else is wrapped
	err = ErrBadParameter.Withf("invalid %q: %v", "name", cause)
	assert.ErrorIs(err, ErrBadParameter)
	assert.NotErrorIs(err, cause)
	assert.Equal(`bad parameter: invalid "name": cause`, err.Error())
}
