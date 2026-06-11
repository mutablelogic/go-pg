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
	types "github.com/mutablelogic/go-server/pkg/types"
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
	router.Spec().AddTag("Schemas", "Schema Operations")

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
			).
			Post(
				func(w http.ResponseWriter, r *http.Request) {
					_ = CreateSchema(w, r, manager, r.PathValue("database"))
				},
				"Create a new schema in a specific database",
				openapi.WithTags("Schemas"),
				openapi.WithJSONRequest(jsonschema.MustFor[schema.SchemaMeta]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Schema]()),
				openapi.WithErrorResponse(http.StatusBadRequest, "Invalid request body"),
				openapi.WithErrorResponse(http.StatusNotFound, "Database not found"),
			),
		),
		router.RegisterPath("schema/{database}/{namespace}", nil, httprequest.NewPathItem("Schema", "Manage a specific PostgreSQL schema").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = GetSchema(w, r, manager, r.PathValue("database"), r.PathValue("namespace"))
				},
				"Get a schema in a specific database",
				openapi.WithTags("Schemas"),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Schema]()),
			).
			Delete(
				func(w http.ResponseWriter, r *http.Request) {
					_ = DeleteSchema(w, r, manager, r.PathValue("database"), r.PathValue("namespace"))
				},
				"Delete a schema in a specific database",
				openapi.WithTags("Schemas"),
				openapi.WithQuery(jsonschema.MustFor[ForceQueryParams]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Schema]()),
				openapi.WithErrorResponse(http.StatusNotFound, "Schema not found"),
				openapi.WithErrorResponse(http.StatusBadRequest, "Invalid query parameters"),
				openapi.WithErrorResponse(http.StatusConflict, "Schema has dependent objects"),
			).
			Patch(
				func(w http.ResponseWriter, r *http.Request) {
					_ = UpdateSchema(w, r, manager, r.PathValue("database"), r.PathValue("namespace"))
				},
				"Update a schema in a specific database",
				openapi.WithTags("Schemas"),
				openapi.WithJSONRequest(jsonschema.MustFor[schema.SchemaMeta]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Schema]()),
				openapi.WithErrorResponse(http.StatusBadRequest, "Invalid request body"),
				openapi.WithErrorResponse(http.StatusNotFound, "Schema not found"),
				openapi.WithErrorResponse(http.StatusBadRequest, "Invalid query parameters"),
				openapi.WithErrorResponse(http.StatusConflict, "Schema has dependent objects"),
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

func GetSchema(w http.ResponseWriter, r *http.Request, manager *manager.Manager, database, namespace string) error {
	if schema, err := manager.GetSchema(r.Context(), database, namespace); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), schema)
	}
}

func CreateSchema(w http.ResponseWriter, r *http.Request, manager *manager.Manager, database string) error {
	var req schema.SchemaMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if schema, err := manager.CreateSchema(r.Context(), database, req); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), schema)
	}
}

func DeleteSchema(w http.ResponseWriter, r *http.Request, manager *manager.Manager, database, namespace string) error {
	var query ForceQueryParams
	if err := httprequest.Query(r.URL.Query(), &query); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if schema, err := manager.DeleteSchema(r.Context(), database, namespace, query.Force); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), schema)
	}
}

func UpdateSchema(w http.ResponseWriter, r *http.Request, manager *manager.Manager, database, namespace string) error {
	var req schema.SchemaMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if schema, err := manager.UpdateSchema(r.Context(), database, namespace, req); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), schema)
	}
}
