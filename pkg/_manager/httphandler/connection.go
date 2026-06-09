package httphandler

import (
	"net/http"
	"strconv"

	// Packages
	manager "github.com/mutablelogic/go-pg/pkg/manager"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterConnectionHandlers registers HTTP handlers for connection operations
// on the provided router with the given path prefix. The manager must be non-nil.
func RegisterConnectionHandlers(router *http.ServeMux, prefix string, manager *manager.Manager) {
	if manager == nil {
		panic("manager is nil")
	}
	router.HandleFunc(joinPath(prefix, "connection"), func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = connectionList(w, r, manager)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	router.HandleFunc(joinPath(prefix, "connection/{pid}"), func(w http.ResponseWriter, r *http.Request) {
		pid, err := strconv.ParseUint(r.PathValue("pid"), 10, 64)
		if err != nil {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing or invalid pid"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			_ = connectionGet(w, r, manager, pid)
		case http.MethodDelete:
			_ = connectionDelete(w, r, manager, pid)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func connectionList(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	// Parse request
	var req schema.ConnectionListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// List the connections
	response, err := manager.ListConnections(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

func connectionGet(w http.ResponseWriter, r *http.Request, manager *manager.Manager, pid uint64) error {
	connection, err := manager.GetConnection(r.Context(), pid)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), connection)
}

func connectionDelete(w http.ResponseWriter, r *http.Request, manager *manager.Manager, pid uint64) error {
	_, err := manager.DeleteConnection(r.Context(), pid)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.Empty(w, http.StatusOK)
}
