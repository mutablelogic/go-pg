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

////////////////////////////////////////////////////////////////////////////////
// TYPES

type RolePathParams struct {
	Role string `json:"role"`
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterRoleHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Roles", "Role Operations")

	return errors.Join(
		router.RegisterPath("role", nil, httprequest.NewPathItem("Roles", "Manage PostgreSQL roles").Tag("Roles").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListRoles(w, r, manager)
				},
				func(op httprequest.PathOperation) {
					op.Summary("List roles")
					op.Query(jsonschema.MustFor[schema.RoleListRequest]())
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.RoleList]())
				},
			).
			Post(
				func(w http.ResponseWriter, r *http.Request) {
					_ = CreateRole(w, r, manager)
				},
				func(op httprequest.PathOperation) {
					op.Summary("Create role")
					op.RequestBody(jsonschema.MustFor[schema.RoleMeta]())
					op.JSONResponse(http.StatusCreated, jsonschema.MustFor[schema.Role]())
				},
			),
		),
		router.RegisterPath("role/{role}", jsonschema.MustFor[RolePathParams](), httprequest.NewPathItem("Role", "Manage a PostgreSQL role").Tag("Roles").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = GetRole(w, r, manager, r.PathValue("role"))
				},
				func(op httprequest.PathOperation) {
					op.Summary("Get role")
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Role]())
					op.ErrorResponse(http.StatusNotFound, "Role not found")
				},
			).
			Delete(
				func(w http.ResponseWriter, r *http.Request) {
					_ = DeleteRole(w, r, manager, r.PathValue("role"))
				},
				func(op httprequest.PathOperation) {
					op.Summary("Delete role")
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Role]())
					op.ErrorResponse(http.StatusNotFound, "Role not found")
				},
			).
			Patch(
				func(w http.ResponseWriter, r *http.Request) {
					_ = UpdateRole(w, r, manager, r.PathValue("role"))
				},
				func(op httprequest.PathOperation) {
					op.Summary("Update role")
					op.RequestBody(jsonschema.MustFor[schema.RoleMeta]())
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Role]())
					op.ErrorResponse(http.StatusNotFound, "Role not found")
				},
			),
		),
	)
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ListRoles(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.RoleListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if roles, err := manager.ListRoles(r.Context(), req); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), roles)
	}
}

func CreateRole(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var meta schema.RoleMeta
	if err := httprequest.Read(r, &meta); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if role, err := manager.CreateRole(r.Context(), meta); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err), meta.Name)
	} else {
		return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), role)
	}
}

func GetRole(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	if role, err := manager.GetRole(r.Context(), name); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err), name)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), role)
	}
}

func DeleteRole(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	if role, err := manager.DeleteRole(r.Context(), name); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err), name)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), role)
	}
}

func UpdateRole(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	var meta schema.RoleMeta
	if err := httprequest.Read(r, &meta); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if role, err := manager.UpdateRole(r.Context(), name, meta); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err), name)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), role)
	}
}
