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

// RegisterTablespaceHandlers registers HTTP handlers for tablespace CRUD operations
// on the provided router with the given path prefix. The manager must be non-nil.
func RegisterTablespaceHandlers(router *http.ServeMux, prefix string, manager *manager.Manager) {
	if manager == nil {
		panic("manager is nil")
	}
	router.HandleFunc(joinPath(prefix, "tablespace"), func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = tablespaceList(w, r, manager)
		case http.MethodPost:
			_ = tablespaceCreate(w, r, manager)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	router.HandleFunc(joinPath(prefix, "tablespace/{name}"), func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if name == "" {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing or invalid tablespace name"))
			return
		}
		if strings.HasPrefix(name, "pg_") {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("tablespace name cannot start with reserved prefix 'pg_'"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			_ = tablespaceGet(w, r, manager, name)
		case http.MethodPatch:
			_ = tablespaceUpdate(w, r, manager, name)
		case http.MethodDelete:
			_ = tablespaceDelete(w, r, manager, name)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func tablespaceList(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	// Parse request
	var req schema.TablespaceListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// List the tablespaces
	response, err := manager.ListTablespaces(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

func tablespaceCreate(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	// Parse request
	var req struct {
		Location string `json:"location"`
		schema.TablespaceMeta
	}
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Create the tablespace
	response, err := manager.CreateTablespace(r.Context(), req.TablespaceMeta, req.Location)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), response)
}

func tablespaceGet(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	// Get the tablespace
	response, err := manager.GetTablespace(r.Context(), name)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

func tablespaceDelete(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	// Delete the tablespace
	_, err := manager.DeleteTablespace(r.Context(), name)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.Empty(w, http.StatusOK)
}

func tablespaceUpdate(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	// Parse request
	var req schema.TablespaceMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Update the tablespace
	response, err := manager.UpdateTablespace(r.Context(), name, req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}
