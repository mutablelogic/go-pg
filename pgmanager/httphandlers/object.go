package httphandlers

import (
	"errors"
	"net/http"

	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pgmanager/manager"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ObjectPathParams struct {
	Database string `json:"database"`
	Schema   string `json:"schema"`
	Name     string `json:"name"`
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterObjectHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Objects", "Object Operations")

	return errors.Join(
		router.RegisterPath("object", nil, httprequest.NewPathItem("Objects", "Manage PostgreSQL objects").Tag("Objects").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListObjects(w, r, manager)
				},
				func(op httprequest.PathOperation) {
					op.Summary("List objects")
					op.Query(jsonschema.MustFor[schema.ObjectListRequest]())
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.ObjectList]())
				},
			),
		),
		router.RegisterPath("object/{database}/{schema}/{name}", jsonschema.MustFor[ObjectPathParams](), httprequest.NewPathItem("Objects", "Manage PostgreSQL objects").Tag("Objects").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = GetObject(w, r, manager, r.PathValue("database"), r.PathValue("schema"), r.PathValue("name"))
				},
				func(op httprequest.PathOperation) {
					op.Summary("Get object")
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Object]())
				},
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ListObjects(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.ObjectListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if objects, err := manager.ListObjects(r.Context(), req); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), objects)
	}
}

func GetObject(w http.ResponseWriter, r *http.Request, manager *manager.Manager, database, namespace, name string) error {
	if object, err := manager.GetObject(r.Context(), database, namespace, name); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), object)
	}
}
