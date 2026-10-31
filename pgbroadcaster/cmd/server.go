//go:build !client

package cmd

import (
	// Packages
	pg "github.com/mutablelogic/go-pg"
	broadcaster "github.com/mutablelogic/go-pg/pgbroadcaster"
	httphandler "github.com/mutablelogic/go-pg/pgbroadcaster/httphandler"
	server "github.com/mutablelogic/go-server"
	servercmd "github.com/mutablelogic/go-server/pkg/cmd"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Flags are the server's flags for the broadcaster
type Flags struct {
	// Other flags
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WithManager creates a broadcaster manager
func (flags Flags) WithManager(ctx server.Cmd, conn pg.PoolConn, srv *servercmd.RunServer, fn func(*broadcaster.Manager) error, opts ...broadcaster.Opt) error {
	opts = append([]broadcaster.Opt{broadcaster.WithTracer(ctx.Tracer())}, opts...)
	manager, err := broadcaster.New(ctx.Context(), conn, opts...)
	if err != nil {
		return err
	}

	// Register the HTTP handlers for watching events
	if srv != nil {
		srv.Register(func(router *httprouter.Router) error {
			ctx.Logger().DebugContext(ctx.Context(), "registering pgbroadcaster http handlers")
			return httphandler.RegisterHandlers(manager, router)
		})
	}

	// Call the function with the manager
	return fn(manager)
}
