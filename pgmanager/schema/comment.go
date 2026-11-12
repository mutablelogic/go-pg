package schema

import (
	"context"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// CommentOn sets the comment of an object, of a type, such as DATABASE, or
// SCHEMA, by its name, or removes it, when it's empty
func CommentOn(ctx context.Context, conn pg.Conn, typ, name, comment string) error {
	value := "NULL"
	if comment != "" {
		value = types.Quote(comment)
	}
	return conn.With("type", typ, "name", name, "comment", value).Exec(ctx, commentOn)
}

////////////////////////////////////////////////////////////////////////////////
// SQL

const (
	// COMMENT takes a literal, not a parameter, so the comment is quoted
	commentOn = `COMMENT ON ${type} ${"name"} IS ${comment}`
)
