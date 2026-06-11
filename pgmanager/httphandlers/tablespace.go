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

type TablespacePathParams struct {
	Name string `json:"name" path:"name" validate:"required"`
}

type TablespaceCreateMeta struct {
	schema.TablespaceMeta
	Location string `json:"location" validate:"required" help:"Location for the tablespace"`
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterTablespaceHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Tablespaces", "Cluster Tablespace Operations")

	return errors.Join(
		router.RegisterPath("tablespace", nil, httprequest.NewPathItem("Tablespaces", "Manage PostgreSQL tablespaces").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListTablespaces(w, r, manager)
				},
				"List tablespaces",
				openapi.WithTags("Tablespaces"),
				openapi.WithQuery(jsonschema.MustFor[schema.TablespaceListRequest]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.TablespaceList]()),
			).
			Post(
				func(w http.ResponseWriter, r *http.Request) {
					_ = CreateTablespace(w, r, manager)
				},
				"Create tablespace",
				openapi.WithTags("Tablespaces"),
				openapi.WithJSONRequest(jsonschema.MustFor[TablespaceCreateMeta]()),
				openapi.WithJSONResponse(http.StatusCreated, jsonschema.MustFor[schema.Tablespace]()),
			),
		),
		router.RegisterPath("tablespace/{name}", jsonschema.MustFor[TablespacePathParams](), httprequest.NewPathItem("Tablespace", "Manage a PostgreSQL tablespace").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = GetTablespace(w, r, manager, r.PathValue("name"))
				},
				"Get tablespace",
				openapi.WithTags("Tablespaces"),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Tablespace]()),
				openapi.WithErrorResponse(http.StatusNotFound, "Tablespace not found"),
			).
			Delete(
				func(w http.ResponseWriter, r *http.Request) {
					_ = DeleteTablespace(w, r, manager, r.PathValue("name"))
				},
				"Drop a tablespace",
				openapi.WithTags("Tablespaces"),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Tablespace]()),
				openapi.WithErrorResponse(http.StatusNotFound, "Tablespace not found"),
			).
			Patch(
				func(w http.ResponseWriter, r *http.Request) {
					_ = UpdateTablespace(w, r, manager, r.PathValue("name"))
				},
				"Update a tablespace",
				openapi.WithTags("Tablespaces"),
				openapi.WithJSONRequest(jsonschema.MustFor[schema.TablespaceMeta]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Tablespace]()),
				openapi.WithErrorResponse(http.StatusNotFound, "Tablespace not found"),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ListTablespaces(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.TablespaceListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if tablespaces, err := manager.ListTablespaces(r.Context(), req); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), tablespaces)
	}
}

func CreateTablespace(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req TablespaceCreateMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if tablespace, err := manager.CreateTablespace(r.Context(), req.TablespaceMeta, req.Location); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), tablespace)
	}
}

func GetTablespace(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	if tablespace, err := manager.GetTablespace(r.Context(), name); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err), name)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), tablespace)
	}
}

func DeleteTablespace(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	if tablespace, err := manager.DeleteTablespace(r.Context(), name); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err), name)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), tablespace)
	}
}

func UpdateTablespace(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	var req schema.TablespaceMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()), name)
	}
	if tablespace, err := manager.UpdateTablespace(r.Context(), name, req); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err), name)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), tablespace)
	}
}
