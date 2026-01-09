package httphandler

import (
	"net/http"
	"strconv"

	// Packages
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	"github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterTaskHandlers registers HTTP handlers for task operations
func RegisterTaskHandlers(router *http.ServeMux, prefix string, manager *queue.Manager) {
	// GET /task lists tasks (with optional queue/status/offset/limit params)
	// POST /task creates a new task
	router.HandleFunc(joinPath(prefix, "task"), func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = taskList(w, r, manager)
		case http.MethodPost:
			_ = taskCreate(w, r, manager)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	// GET /task/{queue}?worker=XX retains next available task from queue (queue cannot start with digit)
	// PATCH /task/{id} marks a task as succeeded or failed with a payload
	router.HandleFunc(joinPath(prefix, "task/{queue_or_id}"), func(w http.ResponseWriter, r *http.Request) {
		pathValue := r.PathValue("queue_or_id")

		switch r.Method {
		case http.MethodGet:
			if !types.IsIdentifier(pathValue) {
				_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("invalid queue name"), pathValue)
				return
			}
			_ = taskRetain(w, r, manager, pathValue)
		case http.MethodPatch:
			id, err := strconv.ParseUint(pathValue, 10, 64)
			if err != nil {
				_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("invalid task id"), pathValue)
				return
			}
			_ = taskRelease(w, r, manager, id)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func taskList(w http.ResponseWriter, r *http.Request, manager *queue.Manager) error {
	// Parse request
	var req schema.TaskListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// List the tasks
	response, err := manager.ListTasks(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

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

func taskRetain(w http.ResponseWriter, r *http.Request, manager *queue.Manager, queue string) error {
	// Parse worker from query
	worker := r.URL.Query().Get("worker")
	if worker == "" {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing worker parameter"))
	}

	// Retain the task
	task, err := manager.NextTask(r.Context(), queue, worker)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	} else if task == nil {
		return httpresponse.Error(w, httpresponse.ErrNotFound.With("no task available"))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), task)
}

func taskRelease(w http.ResponseWriter, r *http.Request, manager *queue.Manager, id uint64) error {
	// Parse request body if present
	var req struct {
		Result any  `json:"result,omitempty"`
		Fail   bool `json:"fail,omitempty"`
	}
	if r.Method == http.MethodPatch {
		if err := httprequest.Read(r, &req); err != nil {
			return httpresponse.Error(w, err)
		}
	}

	// Release the task
	var status string
	task, err := manager.ReleaseTask(r.Context(), id, !req.Fail, req.Result, &status)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success - embed status
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), schema.TaskWithStatus{
		Task:   *task,
		Status: status,
	})
}
