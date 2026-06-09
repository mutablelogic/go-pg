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

// RegisterReplicationSlotHandlers registers HTTP handlers for replication slot
// CRUD operations on the provided router with the given path prefix.
func RegisterReplicationSlotHandlers(router *http.ServeMux, prefix string, manager *manager.Manager) {
	if manager == nil {
		panic("manager is nil")
	}
	router.HandleFunc(joinPath(prefix, "replicationslot"), func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = replicationSlotList(w, r, manager)
		case http.MethodPost:
			_ = replicationSlotCreate(w, r, manager)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	router.HandleFunc(joinPath(prefix, "replicationslot/{name}"), func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if name == "" {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing or invalid slot name"))
			return
		}
		if strings.HasPrefix(name, "pg_") {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("slot name cannot start with reserved prefix 'pg_'"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			_ = replicationSlotGet(w, r, manager, name)
		case http.MethodDelete:
			_ = replicationSlotDelete(w, r, manager, name)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func replicationSlotGet(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	slot, err := manager.GetReplicationSlot(r.Context(), name)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), slot)
}

func replicationSlotList(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	// Parse request
	var req schema.ReplicationSlotListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// List the slots
	response, err := manager.ListReplicationSlots(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

func replicationSlotCreate(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	// Parse request
	var req schema.ReplicationSlotMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Create the slot
	response, err := manager.CreateReplicationSlot(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), response)
}

func replicationSlotDelete(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	// Delete the slot
	_, err := manager.DeleteReplicationSlot(r.Context(), name)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.Empty(w, http.StatusOK)
}
