//go:build !client

package cmd

import (
	// Packages
	pg "github.com/mutablelogic/go-pg"
	pgqueue "github.com/mutablelogic/go-pg/pgqueue"
	httphandler "github.com/mutablelogic/go-pg/pgqueue/httphandler"
	server "github.com/mutablelogic/go-server"
	servercmd "github.com/mutablelogic/go-server/pkg/cmd"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Flags struct {
	// Other flags
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WithManager creates a task queue manager, with the server's tracer and
// meter, and then the options, such as the database schema, registers its
// HTTP handlers with the server, when there is one, and then calls the
// function with it, returning any error
func (flags Flags) WithManager(ctx server.Cmd, conn pg.PoolConn, srv *servercmd.RunServer, fn func(*pgqueue.Manager) error, opts ...pgqueue.Opt) error {
	opts = append([]pgqueue.Opt{pgqueue.WithTracer(ctx.Tracer()), pgqueue.WithMeter(ctx.Meter())}, opts...)
	manager, err := pgqueue.New(ctx.Context(), conn, opts...)
	if err != nil {
		return err
	}

	// Register the HTTP handlers for the queues, tickers and tasks
	if srv != nil {
		srv.Register(func(router *httprouter.Router) error {
			ctx.Logger().DebugContext(ctx.Context(), "registering queue http handlers")
			return httphandler.RegisterHandlers(manager, router)
		})
	}

	// Call the function with the manager
	return fn(manager)
}
