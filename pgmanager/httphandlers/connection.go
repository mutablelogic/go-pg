package httphandlers

import (
	"errors"
	"net/http"
	"strconv"

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

type ConnectionPathParams struct {
	Pid uint32 `json:"pid" path:"pid" validate:"required"`
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterConnectionHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Connections", "Cluster Connection Operations")

	return errors.Join(
		router.RegisterPath("connection", nil, httprequest.NewPathItem("Connections", "Manage PostgreSQL connections").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListConnections(w, r, manager)
				},
				"List connections",
				openapi.WithTags("Connections"),
				openapi.WithQuery(jsonschema.MustFor[schema.ConnectionListRequest]()),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.ConnectionList]()),
			),
		),
		router.RegisterPath("connection/{pid}", jsonschema.MustFor[ConnectionPathParams](), httprequest.NewPathItem("Connection", "Manage a PostgreSQL connection").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = GetConnection(w, r, manager, r.PathValue("pid"))
				},
				"Get connection",
				openapi.WithTags("Connections"),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Connection]()),
				openapi.WithErrorResponse(http.StatusNotFound, "Process not found"),
			).
			Delete(
				func(w http.ResponseWriter, r *http.Request) {
					_ = DeleteConnection(w, r, manager, r.PathValue("pid"))
				},
				"Drop a connection",
				openapi.WithTags("Connections"),
				openapi.WithJSONResponse(http.StatusOK, jsonschema.MustFor[schema.Connection]()),
				openapi.WithErrorResponse(http.StatusNotFound, "Process not found"),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ListConnections(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.ConnectionListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if connections, err := manager.ListConnections(r.Context(), req); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), connections)
	}
}

func GetConnection(w http.ResponseWriter, r *http.Request, manager *manager.Manager, pid string) error {
	pid_, err := strconv.ParseUint(pid, 10, 64)
	if err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With("invalid pid"), pid)
	}
	if connection, err := manager.GetConnection(r.Context(), pid_); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err), pid)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), connection)
	}
}

func DeleteConnection(w http.ResponseWriter, r *http.Request, manager *manager.Manager, pid string) error {
	pid_, err := strconv.ParseUint(pid, 10, 64)
	if err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With("invalid pid"), pid)
	}
	if connection, err := manager.DeleteConnection(r.Context(), pid_); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err), pid)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), connection)
	}
}
