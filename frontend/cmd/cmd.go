//go:build !client

// Package cmd serves the web UI, and the server's configuration, which it
// reads, beside the API, when the server is built with the "frontend" tag,
// which embeds the UI, once it's built, with npm, into build/frontend, which
// make does. Without it, such as with go install, the server serves its API
// and configuration, without the UI.
package cmd

import (
	"context"
	"log/slog"

	// Packages
	server "github.com/mutablelogic/go-server"
	config "github.com/mutablelogic/go-server/config"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Flags are the web UI's flags
type Flags struct {
	UI bool `name:"ui" help:"Serve the web UI from this server, when it's built in." default:"true" negatable:""`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// BuiltIn returns whether the web UI is built in, with the "frontend" tag
func BuiltIn() bool {
	return builtIn
}

// RegisterUI registers the server's configuration, at /config.json, and the
// web UI, at /, when it's built in, unless it isn't to be served. When
// openapi is true, the configuration says where the API's documentation is.
// Returns true when the UI is served, so the server doesn't register its
// own handler of /.
func (f *Flags) RegisterUI(ctx server.Cmd, log *slog.Logger, router *httprouter.Router, openapi bool) (bool, error) {
	if err := config.RegisterHandler(router, config.New(ctx, router, openapi)); err != nil {
		return false, err
	} else if !f.UI {
		return false, nil
	} else if !builtIn {
		log.DebugContext(context.Background(), "the web UI isn't built in, so isn't served, which make, with npm, builds it into")
		return false, nil
	}
	log.DebugContext(context.Background(), "registering web UI handlers")
	return true, registerFS(router)
}
