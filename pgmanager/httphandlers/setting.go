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
		router.RegisterPath("setting/{setting}", nil, httprequest.NewPathItem("Settings", "Manage a PostgreSQL setting").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = GetSetting(w, r, manager, r.PathValue("setting"))
				},
				"Get a setting by name",
				openapi.WithTags("Settings"),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Setting]()),
			).
			Patch(
				func(w http.ResponseWriter, r *http.Request) {
					_ = UpdateSetting(w, r, manager, r.PathValue("setting"))
				},
				"Update a setting by name",
				openapi.WithTags("Settings"),
				openapi.WithJSONRequest(jsonschema.MustFor[schema.SettingMeta]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Setting]()),
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

func GetSetting(w http.ResponseWriter, r *http.Request, manager *manager.Manager, setting string) error {
	if result, err := manager.GetSetting(r.Context(), setting); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), result)
	}
}

func UpdateSetting(w http.ResponseWriter, r *http.Request, manager *manager.Manager, setting string) error {
	var req schema.SettingMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if result, err := manager.UpdateSetting(r.Context(), setting, req); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), result)
	}
}
