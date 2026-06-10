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

type RolePathParams struct {
	Role string `json:"role"`
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterRoleHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Roles", "Role Operations")

	return errors.Join(
		router.RegisterPath("role", nil, httprequest.NewPathItem("Roles", "Manage PostgreSQL roles").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListRoles(w, r, manager)
				},
				"List roles",
				openapi.WithTags("Roles"),
				openapi.WithQuery(jsonschema.MustFor[schema.RoleListRequest]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.RoleList]()),
			).
			Post(
				func(w http.ResponseWriter, r *http.Request) {
					_ = CreateRole(w, r, manager)
				},
				"Create role",
				openapi.WithTags("Roles"),
				openapi.WithJSONRequest(jsonschema.MustFor[schema.RoleMeta]()),
				openapi.WithJSONResponse(http.StatusCreated, jsonschema.MustFor[schema.Role]()),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
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
