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

func RegisterSettingHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Settings", "Settings Operations")

	return errors.Join(
		router.RegisterPath("setting", nil, httprequest.NewPathItem("Settings", "Manage PostgreSQL settings").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListSettings(w, r, manager)
				},
				"List settings",
				openapi.WithTags("Settings"),
				openapi.WithQuery(jsonschema.MustFor[schema.SettingListRequest]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.SettingList]()),
			),
		),
		router.RegisterPath("setting/category", nil, httprequest.NewPathItem("Settings", "Manage PostgreSQL setting categories").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListSettingCategories(w, r, manager)
				},
				"List setting categories",
				openapi.WithTags("Settings"),
				openapi.WithQuery(jsonschema.MustFor[schema.SettingCategoryListRequest]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.SettingCategoryList]()),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ListSettings(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.SettingListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if settings, err := manager.ListSettings(r.Context(), req); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), settings)
	}
}

func ListSettingCategories(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	if categories, err := manager.ListSettingCategories(r.Context(), schema.SettingCategoryListRequest{}); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), categories)
	}
}
