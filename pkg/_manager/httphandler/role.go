package httphandler

import (
	"net/http"
	"strings"

	// Packages
	manager "github.com/mutablelogic/go-pg/pkg/manager"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterRoleHandlers registers HTTP handlers for role CRUD operations
// on the provided router with the given path prefix. The manager must be non-nil.
func RegisterRoleHandlers(router *http.ServeMux, prefix string, manager *manager.Manager) {
	if manager == nil {
		panic("manager is nil")
	}
	router.HandleFunc(joinPath(prefix, "role"), func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = roleList(w, r, manager)
		case http.MethodPost:
			_ = roleCreate(w, r, manager)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	router.HandleFunc(joinPath(prefix, "role/{name}"), func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if name == "" {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing or invalid role name"))
			return
		}
		if strings.HasPrefix(name, "pg_") {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("role name cannot start with reserved prefix 'pg_'"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			_ = roleGet(w, r, manager, name)
		case http.MethodPatch:
			_ = roleUpdate(w, r, manager, name)
		case http.MethodDelete:
			_ = roleDelete(w, r, manager, name)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func roleGet(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	role, err := manager.GetRole(r.Context(), name)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), role)
}

func roleList(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	// Parse request
	var req schema.RoleListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// List the roles
	response, err := manager.ListRoles(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

func roleCreate(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	// Parse request
	var req schema.RoleMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Create the role
	response, err := manager.CreateRole(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), response)
}

func roleDelete(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	_, err := manager.DeleteRole(r.Context(), name)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.Empty(w, http.StatusOK)
}

func roleUpdate(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	// Parse request
	var req schema.RoleMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Perform update
	role, err := manager.UpdateRole(r.Context(), name, req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), role)
}
