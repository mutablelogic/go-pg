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
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterSettingHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Settings", "Settings Operations")

	return errors.Join(
		router.RegisterPath("setting", nil, httprequest.NewPathItem("Settings", "Manage PostgreSQL settings").Tag("Settings").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListSettings(w, r, manager)
				},
				func(op httprequest.PathOperation) {
					op.Summary("List settings")
					op.Query(jsonschema.MustFor[schema.SettingListRequest]())
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.SettingList]())
				},
			),
		),
		router.RegisterPath("setting/category", nil, httprequest.NewPathItem("Settings", "Manage PostgreSQL setting categories").Tag("Settings").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListSettingCategories(w, r, manager)
				},
				func(op httprequest.PathOperation) {
					op.Summary("List setting categories")
					op.Query(jsonschema.MustFor[schema.SettingCategoryListRequest]())
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.SettingCategoryList]())
				},
			),
		),
		router.RegisterPath("setting/reload", nil, httprequest.NewPathItem("Reload", "Reload the PostgreSQL configuration").Tag("Settings").
			Post(
				func(w http.ResponseWriter, r *http.Request) {
					_ = Reload(w, r, manager)
				},
				func(op httprequest.PathOperation) {
					op.Summary("Reload the configuration files")
					op.Description("Reloads the configuration files, with pg_reload_conf(), and returns once they're reloaded, with the settings which wait for a restart, and the errors in the files, whose settings are skipped")
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.SettingReload]())
				},
			),
		),
		router.RegisterPath("setting/restart", nil, httprequest.NewPathItem("Restart", "Restart the PostgreSQL server").Tag("Settings").
			Post(
				func(w http.ResponseWriter, r *http.Request) {
					_ = Restart(w, r, manager)
				},
				func(op httprequest.PathOperation) {
					op.Summary("Restart the server")
					op.Description("Signals the server's postmaster to shut down fast, which ends every session, and returns once a supervisor has started it again. It's not available (503) unless pgmanager runs beside the server, with --pg.restart")
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Restart]())
				},
			),
		),
		router.RegisterPath("setting/{setting}", nil, httprequest.NewPathItem("Settings", "Manage a PostgreSQL setting").Tag("Settings").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = GetSetting(w, r, manager, r.PathValue("setting"))
				},
				func(op httprequest.PathOperation) {
					op.Summary("Get a setting by name")
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Setting]())
				},
			).
			Patch(
				func(w http.ResponseWriter, r *http.Request) {
					_ = UpdateSetting(w, r, manager, r.PathValue("setting"))
				},
				func(op httprequest.PathOperation) {
					op.Summary("Update a setting by name")
					op.RequestBody(jsonschema.MustFor[schema.SettingMeta]())
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Setting]())
				},
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

func Reload(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	if result, err := manager.Reload(r.Context()); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), result)
	}
}

func Restart(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	if result, err := manager.Restart(r.Context()); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), result)
	}
}
