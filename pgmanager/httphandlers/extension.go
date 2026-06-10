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

func RegisterExtensionHandlers(manager *manager.Manager, router *httprouter.Router) error {
	return errors.Join(
		router.RegisterPath("extension", nil, httprequest.NewPathItem("Extensions", "Manage PostgreSQL extensions").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListExtensions(w, r, manager)
				},
				"List extensions",
				openapi.WithTags("Extensions"),
				openapi.WithQuery(jsonschema.MustFor[schema.ExtensionListRequest]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.ExtensionList]()),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ListExtensions(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.ExtensionListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if extensions, err := manager.ListExtensions(r.Context(), req); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), extensions)
	}
}
