package httphandler

import (
	"net/http"
	"strconv"

	// Packages
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterTaskHandlers registers HTTP handlers for task operations
func RegisterTaskHandlers(router *http.ServeMux, prefix string, manager *queue.Manager) {
	// GET /task retains a task from any queue
	// POST /task creates a new task
	router.HandleFunc(joinPath(prefix, "task"), func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = taskRetain(w, r, manager)
		case http.MethodPost:
			_ = taskCreate(w, r, manager)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	// PATCH /task/{id} marks a task as succeeded or failed with a payload
	// DELETE /task/{id} marks a task as succeeded
	router.HandleFunc(joinPath(prefix, "task/{id}"), func(w http.ResponseWriter, r *http.Request) {
		// Check id parameter
		id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
		if err != nil {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing or invalid task id"), r.PathValue("id"))
			return
		}

		switch r.Method {
		case http.MethodPatch:
			_ = taskRelease(w, r, manager, id)
		case http.MethodDelete:
			_ = taskRelease(w, r, manager, id)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func taskCreate(w http.ResponseWriter, r *http.Request, manager *queue.Manager) error {
	// Parse request
	var req struct {
		Queue string `json:"queue"`
		schema.TaskMeta
	}
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Check queue name
	q, err := manager.GetQueue(r.Context(), req.Queue)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Create the task
	task, err := manager.CreateTask(r.Context(), q.Queue, req.TaskMeta)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), task)
}

func taskRetain(w http.ResponseWriter, r *http.Request, manager *queue.Manager) error {
	// Parse request
	var req struct {
		Queue  string `json:"queue"`
		Worker string `json:"worker"`
	}
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Retain the task
	task, err := manager.NextTask(r.Context(), req.Queue, req.Worker)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	} else if task == nil {
		return httpresponse.Error(w, httpresponse.ErrNotFound.With("no task available"))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), task)
}

func taskRelease(w http.ResponseWriter, r *http.Request, manager *queue.Manager, id uint64) error {
	// Only parse request if method is PATCH
	var req struct {
		Result any `json:"result,omitempty"`
	}
	if r.Method == http.MethodPatch {
		if err := httprequest.Read(r, &req); err != nil {
			return httpresponse.Error(w, err)
		}
	}

	// Release the task - success if no result
	var status string
	task, err := manager.ReleaseTask(r.Context(), id, req.Result == nil, req.Result, &status)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success - embed status
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), schema.TaskWithStatus{
		Task:   *task,
		Status: status,
	})
}
