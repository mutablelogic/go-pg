package httphandlers

import (
	"errors"
	"net/http"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pgmanager/manager"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterStatementHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Statements", "Observe PostgreSQL statement execution statistics")

	return errors.Join(
		router.RegisterPath("statement", nil, httprequest.NewPathItem("Statements", "Observe PostgreSQL statement execution statistics").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListStatements(w, r, manager)
				},
				"List statements",
				openapi.WithTags("Statements"),
				openapi.WithQuery(jsonschema.MustFor[schema.StatementListRequest]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.StatementList]()),
			).
			Delete(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ResetStatementStats(w, r, manager)
				},
				"Reset statement statistics",
				openapi.WithTags("Statements"),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ListStatements(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.StatementListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if statements, err := manager.ListStatements(r.Context(), req); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), statements)
	}
}

func ResetStatementStats(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	if err := manager.ResetStatements(r.Context()); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.Empty(w, http.StatusNoContent)
	}
}
