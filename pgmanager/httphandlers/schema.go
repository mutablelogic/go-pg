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
	"github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type SchemaPathParams struct {
	Database  string `json:"database"`
	Namespace string `json:"namespace"`
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterSchemaHandlers(manager *manager.Manager, router *httprouter.Router) error {
	return errors.Join(
		router.RegisterPath("schema", nil, httprequest.NewPathItem("Schemas", "Manage PostgreSQL schemas").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListSchemas(w, r, manager, nil)
				},
				"List all schemas",
				openapi.WithTags("Schemas"),
				openapi.WithQuery(jsonschema.MustFor[pg.OffsetLimit]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.SchemaList]()),
			),
		),
		router.RegisterPath("schema/{database}", nil, httprequest.NewPathItem("Schemas", "Manage PostgreSQL schemas").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListSchemas(w, r, manager, types.Ptr(r.PathValue("database")))
				},
				"List schemas in a specific database",
				openapi.WithTags("Schemas"),
				openapi.WithQuery(jsonschema.MustFor[pg.OffsetLimit]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.SchemaList]()),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ListSchemas(w http.ResponseWriter, r *http.Request, manager *manager.Manager, database *string) error {
	var req pg.OffsetLimit
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if schemas, err := manager.ListSchemas(r.Context(), schema.SchemaListRequest{
		OffsetLimit: req,
		Database:    database,
	}); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), schemas)
	}
}
