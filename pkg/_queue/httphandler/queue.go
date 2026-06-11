package httphandler

import (
	"errors"
	"net/http"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterQueueHandlers registers HTTP handlers for queue CRUD operations
// on the provided router with the given path prefix. The manager must be non-nil.
func RegisterQueueHandlers(router *http.ServeMux, prefix string, manager *queue.Manager, middleware HTTPMiddlewareFuncs) {
	router.HandleFunc(joinPath(prefix, "queue"), middleware.Wrap(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = queueList(w, r, manager)
		case http.MethodPost:
			_ = queueCreate(w, r, manager)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	}))

	router.HandleFunc(joinPath(prefix, "queue/{name}"), middleware.Wrap(func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		switch r.Method {
		case http.MethodGet:
			_ = queueGet(w, r, manager, name)
		case http.MethodPatch:
			_ = queueUpdate(w, r, manager, name)
		case http.MethodDelete:
			_ = queueDelete(w, r, manager, name)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	}))
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func queueGet(w http.ResponseWriter, r *http.Request, manager *queue.Manager, name string) error {
	queue, err := manager.GetQueue(r.Context(), name)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), queue)
}

func queueList(w http.ResponseWriter, r *http.Request, manager *queue.Manager) error {
	// Parse request
	var req schema.QueueListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// List the queues
	response, err := manager.ListQueues(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

func queueCreate(w http.ResponseWriter, r *http.Request, manager *queue.Manager) error {
	// Parse request
	var req schema.QueueMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// If the queue already exists, return an error
	if _, err := manager.GetQueue(r.Context(), req.Queue); err == nil {
		return httpresponse.Error(w, httpresponse.ErrConflict.With("queue already exists"), req.Queue)
	} else if !errors.Is(err, pg.ErrNotFound) {
		return httpresponse.Error(w, httperr(err))
	}

	// Register the queue
	response, err := manager.RegisterQueue(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), response)
}

func queueUpdate(w http.ResponseWriter, r *http.Request, manager *queue.Manager, name string) error {
	// Parse request
	var meta schema.QueueMeta
	if err := httprequest.Read(r, &meta); err != nil {
		return httpresponse.Error(w, err)
	}

	// Perform update
	queue, err := manager.UpdateQueue(r.Context(), name, meta)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), queue)
}

func queueDelete(w http.ResponseWriter, r *http.Request, manager *queue.Manager, name string) error {
	queue, err := manager.DeleteQueue(r.Context(), name)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), queue)
}
