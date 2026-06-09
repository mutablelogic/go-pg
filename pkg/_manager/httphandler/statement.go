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

// RegisterStatementHandlers registers HTTP handlers for pg_stat_statements operations
// on the provided router with the given path prefix. The manager must be non-nil.
func RegisterStatementHandlers(router *http.ServeMux, prefix string, manager *manager.Manager) {
	if manager == nil {
		panic("manager is nil")
	}

	// List statements or reset statistics
	router.HandleFunc(joinPath(prefix, "statement"), func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = statementList(w, r, manager)
		case http.MethodDelete:
			_ = statementReset(w, r, manager)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func statementList(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	// Parse request
	var req schema.StatementListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// List the statements
	response, err := manager.ListStatements(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

func statementReset(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	// Reset the statements
	if err := manager.ResetStatements(r.Context()); err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success (no content)
	return httpresponse.Empty(w, http.StatusNoContent)
}
