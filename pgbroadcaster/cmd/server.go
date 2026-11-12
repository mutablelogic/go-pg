//go:build !client

// Package cmd has the broadcaster's flags for a server. Its changes are
// watched with go-server's broadcaster/cmd, such as its changes command.
package cmd

import (
	// Packages
	pg "github.com/mutablelogic/go-pg"
	broadcaster "github.com/mutablelogic/go-pg/pgbroadcaster"
	server "github.com/mutablelogic/go-server"
	httphandler "github.com/mutablelogic/go-server/broadcaster/httphandler"
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

// WithManager creates a broadcaster manager, registers its broadcaster's
// HTTP handler, which streams its changes at /changes, with the server,
// when there is one, and then calls the function with the manager. A
// broadcaster which is shared with the manager, with WithBroadcaster, isn't
// registered, as whatever shares it registers it.
func (flags Flags) WithManager(ctx server.Cmd, conn pg.PoolConn, srv *servercmd.RunServer, fn func(*broadcaster.Manager) error, opts ...broadcaster.Opt) error {
	opts = append([]broadcaster.Opt{broadcaster.WithTracer(ctx.Tracer())}, opts...)
	manager, err := broadcaster.New(ctx.Context(), conn, opts...)
	if err != nil {
		return err
	}

	// Register the HTTP handler for watching the changes
	if srv != nil && !manager.Shared() {
		srv.Register(func(router *httprouter.Router) error {
			ctx.Logger().DebugContext(ctx.Context(), "registering pgbroadcaster http handlers")
			return httphandler.RegisterHandlers(manager.Broadcaster(), router)
		})
	}

	// Call the function with the manager
	return fn(manager)
}
