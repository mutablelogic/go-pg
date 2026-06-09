package httphandler

import (
	"net/http"

	// Packages
	manager "github.com/mutablelogic/go-pg/pkg/manager"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterSettingHandlers registers HTTP handlers for server setting operations
// on the provided router with the given path prefix. The manager must be non-nil.
func RegisterSettingHandlers(router *http.ServeMux, prefix string, manager *manager.Manager) {
	if manager == nil {
		panic("manager is nil")
	}

	// List settings
	router.HandleFunc(joinPath(prefix, "setting"), func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = settingList(w, r, manager)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	// List setting categories
	router.HandleFunc(joinPath(prefix, "setting/category"), func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = settingCategoryList(w, r, manager)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	// Get or update a specific setting
	router.HandleFunc(joinPath(prefix, "setting/{name}"), func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if name == "" {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing or invalid setting name"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			_ = settingGet(w, r, manager, name)
		case http.MethodPatch:
			_ = settingUpdate(w, r, manager, name)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func settingGet(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	setting, err := manager.GetSetting(r.Context(), name)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), setting)
}

func settingList(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	// Parse request
	var req schema.SettingListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// List the settings
	response, err := manager.ListSettings(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

func settingCategoryList(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	// List the setting categories
	response, err := manager.ListSettingCategories(r.Context())
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

func settingUpdate(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	// Parse query for reload option
	var opts struct {
		Reload bool `json:"reload,omitempty" help:"Reload config after update"`
	}
	if err := httprequest.Query(r.URL.Query(), &opts); err != nil {
		return httpresponse.Error(w, err)
	}

	// Parse request body
	var req schema.SettingMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Update the setting
	response, err := manager.UpdateSetting(r.Context(), name, req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Reload config if requested and the setting context supports it
	if opts.Reload && response.Context == "sighup" {
		if err := manager.ReloadConfig(r.Context()); err != nil {
			return httpresponse.Error(w, httperr(err))
		}
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}
