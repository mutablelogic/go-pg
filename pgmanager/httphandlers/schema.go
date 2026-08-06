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
		router.RegisterPath("schema", nil, httprequest.NewPathItem("Schemas", "Manage PostgreSQL schemas").Tag("Schemas").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListSchemas(w, r, manager, nil)
				},
				func(op httprequest.PathOperation) {
					op.Summary("List all schemas")
					op.Query(jsonschema.MustFor[pg.OffsetLimit]())
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.SchemaList]())
				},
			),
		),
		router.RegisterPath("schema/{database}", nil, httprequest.NewPathItem("Schemas", "Manage PostgreSQL schemas").Tag("Schemas").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListSchemas(w, r, manager, types.Ptr(r.PathValue("database")))
				},
				func(op httprequest.PathOperation) {
					op.Summary("List schemas in a specific database")
					op.Query(jsonschema.MustFor[pg.OffsetLimit]())
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.SchemaList]())
				},
			).
			Post(
				func(w http.ResponseWriter, r *http.Request) {
					_ = CreateSchema(w, r, manager, r.PathValue("database"))
				},
				func(op httprequest.PathOperation) {
					op.Summary("Create a new schema in a specific database")
					op.RequestBody(jsonschema.MustFor[schema.SchemaMeta]())
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Schema]())
					op.ErrorResponse(http.StatusBadRequest, "Invalid request body")
					op.ErrorResponse(http.StatusNotFound, "Database not found")
				},
			),
		),
		router.RegisterPath("schema/{database}/{namespace}", nil, httprequest.NewPathItem("Schema", "Manage a specific PostgreSQL schema").Tag("Schemas").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = GetSchema(w, r, manager, r.PathValue("database"), r.PathValue("namespace"))
				},
				func(op httprequest.PathOperation) {
					op.Summary("Get a schema in a specific database")
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Schema]())
				},
			).
			Delete(
				func(w http.ResponseWriter, r *http.Request) {
					_ = DeleteSchema(w, r, manager, r.PathValue("database"), r.PathValue("namespace"))
				},
				func(op httprequest.PathOperation) {
					op.Summary("Delete a schema in a specific database")
					op.Query(jsonschema.MustFor[ForceQueryParams]())
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Schema]())
					op.ErrorResponse(http.StatusNotFound, "Schema not found")
					op.ErrorResponse(http.StatusBadRequest, "Invalid query parameters")
					op.ErrorResponse(http.StatusConflict, "Schema has dependent objects")
				},
			).
			Patch(
				func(w http.ResponseWriter, r *http.Request) {
					_ = UpdateSchema(w, r, manager, r.PathValue("database"), r.PathValue("namespace"))
				},
				func(op httprequest.PathOperation) {
					op.Summary("Update a schema in a specific database")
					op.RequestBody(jsonschema.MustFor[schema.SchemaMeta]())
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Schema]())
					op.ErrorResponse(http.StatusBadRequest, "Invalid request body")
					op.ErrorResponse(http.StatusNotFound, "Schema not found")
					op.ErrorResponse(http.StatusBadRequest, "Invalid query parameters")
					op.ErrorResponse(http.StatusConflict, "Schema has dependent objects")
				},
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
