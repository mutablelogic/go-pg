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

type DatabasePathParams struct {
	Database string `json:"database"`
}

type ForceQueryParams struct {
	Force bool `json:"force" query:"force" help:"Force the operation, even when there are active connections to the database."`
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterDatabaseHandlers(manager *manager.Manager, router *httprouter.Router) error {
	return errors.Join(
		router.RegisterPath("database", nil, httprequest.NewPathItem("Databases", "Manage PostgreSQL databases").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListDatabases(w, r, manager)
				},
				"List databases",
				openapi.WithTags("Databases"),
				openapi.WithQuery(jsonschema.MustFor[schema.DatabaseListRequest]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.DatabaseList]()),
			).
			Post(
				func(w http.ResponseWriter, r *http.Request) {
					_ = CreateDatabase(w, r, manager)
				},
				"Create database",
				openapi.WithTags("Databases"),
				openapi.WithJSONRequest(jsonschema.MustFor[schema.DatabaseMeta]()),
				openapi.WithJSONResponse(http.StatusCreated, jsonschema.MustFor[schema.Database]()),
			),
		),

		router.RegisterPath("database/{database}", jsonschema.MustFor[DatabasePathParams](), httprequest.NewPathItem("Database", "Manage a PostgreSQL database").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = GetDatabase(w, r, manager, r.PathValue("database"))
				},
				"Get database",
				openapi.WithTags("Databases"),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Database]()),
				openapi.WithErrorResponse(http.StatusNotFound, "Database not found"),
			).
			Delete(
				func(w http.ResponseWriter, r *http.Request) {
					_ = DeleteDatabase(w, r, manager, r.PathValue("database"))
				},
				"Delete database",
				openapi.WithTags("Databases"),
				openapi.WithQuery(jsonschema.MustFor[ForceQueryParams]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Database]()),
				openapi.WithErrorResponse(http.StatusNotFound, "Database not found"),
			).
			Patch(
				func(w http.ResponseWriter, r *http.Request) {
					_ = UpdateDatabase(w, r, manager, r.PathValue("database"))
				},
				"Update database",
				openapi.WithTags("Databases"),
				openapi.WithJSONRequest(jsonschema.MustFor[schema.DatabaseMeta]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Database]()),
				openapi.WithErrorResponse(http.StatusNotFound, "Database not found"),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ListDatabases(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.DatabaseListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if databases, err := manager.ListDatabases(r.Context(), req); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), databases)
	}
}

func GetDatabase(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	if database, err := manager.GetDatabase(r.Context(), name); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err), name)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), database)
	}
}

func DeleteDatabase(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	var query ForceQueryParams
	if err := httprequest.Query(r.URL.Query(), &query); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if database, err := manager.DeleteDatabase(r.Context(), name, query.Force); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err), name)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), database)
	}
}

func CreateDatabase(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var meta schema.DatabaseMeta
	if err := httprequest.Read(r, &meta); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if database, err := manager.CreateDatabase(r.Context(), meta); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err), meta.Name)
	} else {
		return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), database)
	}
}

func UpdateDatabase(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	var meta schema.DatabaseMeta
	if err := httprequest.Read(r, &meta); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if database, err := manager.UpdateDatabase(r.Context(), name, meta); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err), name)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), database)
	}
}
