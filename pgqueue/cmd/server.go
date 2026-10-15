package cmd

import (
	"context"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	pgqueue "github.com/mutablelogic/go-pg/pgqueue"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Flags struct {
	// Other flags
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (flags Flags) WithManager(ctx context.Context, conn pg.PoolConn, fn func(*pgqueue.Manager) error, opts ...pgqueue.Opt) error {
	// Create a manager and then call the function with the manager, returning any error
	if manager, err := pgqueue.New(ctx, conn, opts...); err != nil {
		return err
	} else {
		return fn(manager)
	}
}
