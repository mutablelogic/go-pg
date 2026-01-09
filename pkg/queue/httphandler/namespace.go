package httphandler

import (
	"net/http"

	// Packages
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterNamespaceHandlers registers namespace-related HTTP handlers
func RegisterNamespaceHandlers(router *http.ServeMux, prefix string, manager *queue.Manager) {
	// List namespaces
	router.HandleFunc(joinPath(prefix, "namespace"), func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = namespaceList(w, r, manager)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func namespaceList(w http.ResponseWriter, r *http.Request, manager *queue.Manager) error {
	// List all namespaces
	namespaces, err := manager.ListNamespaces(r.Context(), schema.NamespaceListRequest{})
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), namespaces)
}
