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
// TYPES

type ReplicationSlotPathParams struct {
	Name string `json:"name" path:"name" validate:"required"`
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterReplicationSlotHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Replication Slots", "Manage PostgreSQL replication slots")

	return errors.Join(
		router.RegisterPath("replication-slot", nil, httprequest.NewPathItem("Replication Slots", "Manage PostgreSQL replication slots").Tag("Replication Slots").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListReplicationSlots(w, r, manager)
				},
				func(op httprequest.PathOperation) {
					op.Summary("List replication slots")
					op.Query(jsonschema.MustFor[schema.ReplicationSlotListRequest]())
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.ReplicationSlotList]())
				},
			).
			Post(
				func(w http.ResponseWriter, r *http.Request) {
					_ = CreateReplicationSlot(w, r, manager)
				},
				func(op httprequest.PathOperation) {
					op.Summary("Create replication slot")
					op.RequestBody(jsonschema.MustFor[schema.ReplicationSlotMeta]())
					op.JSONResponse(http.StatusCreated, jsonschema.MustFor[schema.ReplicationSlot]())
				},
			),
		),
		router.RegisterPath("replication-slot/{name}", jsonschema.MustFor[ReplicationSlotPathParams](), httprequest.NewPathItem("Replication Slot", "Manage a PostgreSQL replication slot").Tag("Replication Slots").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = GetReplicationSlot(w, r, manager, r.PathValue("name"))
				},
				func(op httprequest.PathOperation) {
					op.Summary("Get replication slot")
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.ReplicationSlot]())
					op.ErrorResponse(http.StatusNotFound, "Replication slot not found")
				},
			).
			Delete(
				func(w http.ResponseWriter, r *http.Request) {
					_ = DeleteReplicationSlot(w, r, manager, r.PathValue("name"))
				},
				func(op httprequest.PathOperation) {
					op.Summary("Drop a replication slot")
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.ReplicationSlot]())
					op.ErrorResponse(http.StatusNotFound, "Replication slot not found")
				},
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ListReplicationSlots(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.ReplicationSlotListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if slots, err := manager.ListReplicationSlots(r.Context(), req); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), slots)
	}
}

func CreateReplicationSlot(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var meta schema.ReplicationSlotMeta
	if err := httprequest.Read(r, &meta); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if slot, err := manager.CreateReplicationSlot(r.Context(), meta); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), slot)
	}
}

func GetReplicationSlot(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	if slot, err := manager.GetReplicationSlot(r.Context(), name); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err), name)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), slot)
	}
}

func DeleteReplicationSlot(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	if slot, err := manager.DeleteReplicationSlot(r.Context(), name); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err), name)
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), slot)
	}
}
