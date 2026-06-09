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
// TYPES

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterConnectionHandlers(manager *manager.Manager, router *httprouter.Router) error {
	return errors.Join(
		router.RegisterPath("connection", nil, httprequest.NewPathItem("Connections", "Manage PostgreSQL connections").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListConnections(w, r, manager)
				},
				"List connections",
				openapi.WithTags("Connections"),
				openapi.WithQuery(jsonschema.MustFor[schema.ConnectionListRequest]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.ConnectionList]()),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ListConnections(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.ConnectionListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if connections, err := manager.ListConnections(r.Context(), req); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), connections)
	}
}
