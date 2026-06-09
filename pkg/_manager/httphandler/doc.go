// Package httphandler provides REST API endpoints for PostgreSQL management
// operations.
//
// Register all handlers with an http.ServeMux:
//
//	httphandler.RegisterBackendHandlers(mux, "/api/v1", mgr)
//
// This registers endpoints for roles, databases, schemas, objects, tablespaces,
// extensions, connections, settings, statements, replication slots, and
// Prometheus metrics.
package httphandler
