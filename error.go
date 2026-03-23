package pg

import (
	"errors"
	"fmt"

	// Packages
	pgx "github.com/jackc/pgx/v5"
	pgconn "github.com/jackc/pgx/v5/pgconn"
)

/////////////////////////////////////////////////////////////////////
// TYPES

type Err int

type DatabaseError struct {
	code    string
	message string
	err     error
	kinds   []Err
}

/////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	ErrSuccess Err = iota
	ErrNotFound
	ErrNotImplemented
	ErrBadParameter
	ErrNotAvailable
	ErrConflict
	ErrDatabase
	ErrUniqueViolation
	ErrForeignKeyViolation
	ErrNotNullViolation
	ErrCheckViolation
	ErrInvalidTextRepresentation
	ErrInvalidDatetimeFormat
	ErrDatetimeFieldOverflow
)

const (
	sqlStateUniqueViolation          = "23505"
	sqlStateForeignKeyViolation      = "23503"
	sqlStateNotNullViolation         = "23502"
	sqlStateCheckViolation           = "23514"
	sqlStateInvalidTextRepresentation = "22P02"
	sqlStateInvalidDatetimeFormat    = "22007"
	sqlStateDatetimeFieldOverflow    = "22008"
)

// Error returns the string representation of the error.
func (e Err) Error() string {
	switch e {
	case ErrSuccess:
		return "success"
	case ErrNotFound:
		return "not found"
	case ErrNotImplemented:
		return "not implemented"
	case ErrBadParameter:
		return "bad parameter"
	case ErrNotAvailable:
		return "not available"
	case ErrConflict:
		return "conflict"
	case ErrDatabase:
		return "database error"
	case ErrUniqueViolation:
		return "unique violation"
	case ErrForeignKeyViolation:
		return "foreign key violation"
	case ErrNotNullViolation:
		return "not null violation"
	case ErrCheckViolation:
		return "check violation"
	case ErrInvalidTextRepresentation:
		return "invalid text representation"
	case ErrInvalidDatetimeFormat:
		return "invalid datetime format"
	case ErrDatetimeFieldOverflow:
		return "datetime field overflow"
	default:
		return fmt.Sprint("Unknown error ", int(e))
	}
}

// With returns the error with additional context appended.
func (e Err) With(a ...any) error {
	return fmt.Errorf("%w: %s", e, fmt.Sprint(a...))
}

// Withf returns the error with formatted context appended.
func (e Err) Withf(format string, a ...any) error {
	return fmt.Errorf("%w: %s", e, fmt.Sprintf(format, a...))
}

// Error returns the wrapped database error string.
func (e *DatabaseError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.err != nil {
		return e.err.Error()
	}
	if e.message != "" {
		return e.message
	}
	return e.code
}

// Unwrap returns the underlying driver error.
func (e *DatabaseError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

// Is supports errors.Is checks against broad and specific package errors.
func (e *DatabaseError) Is(target error) bool {
	if e == nil {
		return false
	}
	if kind, ok := target.(Err); ok {
		for _, candidate := range e.kinds {
			if candidate == kind {
				return true
			}
		}
	}
	return errors.Is(e.err, target)
}

// SQLState returns the PostgreSQL SQLSTATE code.
func (e *DatabaseError) SQLState() string {
	if e == nil {
		return ""
	}
	return e.code
}

// Message returns the PostgreSQL error message.
func (e *DatabaseError) Message() string {
	if e == nil {
		return ""
	}
	return e.message
}

/////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// NormalizeError maps driver-specific PostgreSQL errors to package errors.
func NormalizeError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotFound) {
		return err
	}
	var dbErr *DatabaseError
	if errors.As(err, &dbErr) {
		return err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return newDatabaseError(pgErr)
	}
	return err
}

// IsDatabaseError reports whether err is a PostgreSQL error with a SQLSTATE code.
func IsDatabaseError(err error) bool {
	return SQLState(err) != ""
}

// SQLState returns the PostgreSQL SQLSTATE code for err, if one is available.
func SQLState(err error) string {
	if err == nil {
		return ""
	}
	var dbErr *DatabaseError
	if errors.As(err, &dbErr) {
		return dbErr.SQLState()
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func pgerror(err error) error {
	return NormalizeError(err)
}

/////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func newDatabaseError(err *pgconn.PgError) error {
	if err == nil {
		return nil
	}

	kinds := []Err{ErrDatabase}
	switch err.Code {
	case sqlStateUniqueViolation:
		kinds = append(kinds, ErrConflict, ErrUniqueViolation)
	case sqlStateForeignKeyViolation:
		kinds = append(kinds, ErrBadParameter, ErrForeignKeyViolation)
	case sqlStateNotNullViolation:
		kinds = append(kinds, ErrBadParameter, ErrNotNullViolation)
	case sqlStateCheckViolation:
		kinds = append(kinds, ErrBadParameter, ErrCheckViolation)
	case sqlStateInvalidTextRepresentation:
		kinds = append(kinds, ErrBadParameter, ErrInvalidTextRepresentation)
	case sqlStateInvalidDatetimeFormat:
		kinds = append(kinds, ErrBadParameter, ErrInvalidDatetimeFormat)
	case sqlStateDatetimeFieldOverflow:
		kinds = append(kinds, ErrBadParameter, ErrDatetimeFieldOverflow)
	}

	return &DatabaseError{
		code:    err.Code,
		message: err.Message,
		err:     err,
		kinds:   kinds,
	}
}