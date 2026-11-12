package test

import (
	"testing"

	// Packages
	pgqueue "github.com/mutablelogic/go-pg/pgqueue"
	httphandler "github.com/mutablelogic/go-pg/pgqueue/httphandler"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	servertest "github.com/mutablelogic/go-server/test"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Server is a HTTP test server of the pgqueue API, which is go-server's, so
// every request and response is checked against the OpenAPI specification
// when the test ends
type Server = servertest.Server

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

// Prefix is the path prefix for the API routes
const Prefix = servertest.Prefix

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewServer starts a HTTP test server serving the pgqueue API for the
// manager. When the test ends the server is closed, and every recorded
// exchange is checked against the OpenAPI specification.
func NewServer(t *testing.T, manager *pgqueue.Manager) *Server {
	t.Helper()
	return servertest.NewServer(t, func(router *httprouter.Router) error {
		return httphandler.RegisterHandlers(manager, router)
	})
}
