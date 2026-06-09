package httphandler

import (
	"net/http"
	"strings"

	// Packages
	manager "github.com/mutablelogic/go-pg/pkg/manager"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterDatabaseHandlers registers HTTP handlers for database CRUD operations
// on the provided router with the given path prefix. The manager must be non-nil.
func RegisterDatabaseHandlers(router *http.ServeMux, prefix string, manager *manager.Manager) {
	if manager == nil {
		panic("manager is nil")
	}
	router.HandleFunc(joinPath(prefix, "database"), func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = databaseList(w, r, manager)
		case http.MethodPost:
			_ = databaseCreate(w, r, manager)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	router.HandleFunc(joinPath(prefix, "database/{name}"), func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if name == "" {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing or invalid database name"))
			return
		}
		if strings.HasPrefix(name, "pg_") {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("database name cannot start with reserved prefix 'pg_'"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			_ = databaseGet(w, r, manager, name)
		case http.MethodPatch:
			_ = databaseUpdate(w, r, manager, name)
		case http.MethodDelete:
			_ = databaseDelete(w, r, manager, name)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func databaseGet(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	database, err := manager.GetDatabase(r.Context(), name)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), database)
}

func databaseList(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	// Parse request
	var req schema.DatabaseListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// List the databases
	response, err := manager.ListDatabases(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

func databaseCreate(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	// Parse request
	var req schema.DatabaseMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Create the database
	response, err := manager.CreateDatabase(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), response)
}

func databaseDelete(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	// Parse the query
	var req struct {
		Force bool `json:"force,omitempty" help:"Force delete"`
	}
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Delete the database
	_, err := manager.DeleteDatabase(r.Context(), name, req.Force)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.Empty(w, http.StatusOK)
}

func databaseUpdate(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	// Parse request
	var req schema.DatabaseMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Perform update
	database, err := manager.UpdateDatabase(r.Context(), name, req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), database)
}
