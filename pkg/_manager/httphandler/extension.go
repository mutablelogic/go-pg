package httphandler

import (
	"net/http"

	// Packages
	manager "github.com/mutablelogic/go-pg/pkg/manager"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterExtensionHandlers registers HTTP handlers for extension CRUD operations
// on the provided router with the given path prefix. The manager must be non-nil.
func RegisterExtensionHandlers(router *http.ServeMux, prefix string, manager *manager.Manager) {
	if manager == nil {
		panic("manager is nil")
	}
	router.HandleFunc(joinPath(prefix, "extension"), func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = extensionList(w, r, manager)
		case http.MethodPost:
			_ = extensionCreate(w, r, manager)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	router.HandleFunc(joinPath(prefix, "extension/{name}"), func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if name == "" {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing or invalid extension name"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			_ = extensionGet(w, r, manager, name)
		case http.MethodPatch:
			_ = extensionUpdate(w, r, manager, name)
		case http.MethodDelete:
			_ = extensionDelete(w, r, manager, name)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func extensionList(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	// Parse request
	var req schema.ExtensionListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// List the extensions
	response, err := manager.ListExtensions(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

func extensionCreate(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	// Parse request
	var req schema.ExtensionMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Parse cascade from query params
	cascade := r.URL.Query().Get("cascade") == "true"

	// Create the extension
	response, err := manager.CreateExtension(r.Context(), req, cascade)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), response)
}

func extensionGet(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	// Get the extension
	response, err := manager.GetExtension(r.Context(), name)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

func extensionDelete(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	// Parse the query
	var req struct {
		Database string `json:"database" help:"Database to delete extension from"`
		Cascade  bool   `json:"cascade,omitempty" help:"Cascade delete to dependent objects"`
	}
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Delete the extension
	err := manager.DeleteExtension(r.Context(), req.Database, name, req.Cascade)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.Empty(w, http.StatusOK)
}

func extensionUpdate(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	// Parse request
	var req schema.ExtensionMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Update the extension
	response, err := manager.UpdateExtension(r.Context(), name, req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}
