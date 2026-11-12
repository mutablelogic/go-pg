package httphandler

import (
	"errors"
	"net/http"

	// Packages
	pgqueue "github.com/mutablelogic/go-pg/pgqueue"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// queueName is the name of a queue in the path
type queueName struct {
	Name string `json:"name" help:"Queue name"`
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func queueHandlers(manager *pgqueue.Manager, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	return errors.Join(
		queueListHandler(manager, router, documentation),
		queueGetHandler(manager, router, documentation),
		queueCleanHandler(manager, router, documentation),
	)
}

func queueListHandler(manager *pgqueue.Manager, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	return router.Register("queue", nil, func(path httprequest.PathItem) {
		path.Tag("Queue")

		// GET
		path.Get(func(w http.ResponseWriter, r *http.Request) {
			var req schema.QueueListRequest
			if err := httprequest.Query(r.URL.Query(), &req); err != nil {
				httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
			} else if list, err := manager.ListQueues(r.Context(), req); err != nil {
				httpresponse.Error(w, httpErr(err))
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), list)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("List Queues")
			op.Description(documentation.Section(3, "GET /queue").Body)
			op.Query(jsonschema.MustFor[schema.QueueListRequest]())
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.QueueList](), "List of queues")
			op.ErrorResponse(http.StatusBadRequest, "Invalid request")
		})
	})
}

func queueGetHandler(manager *pgqueue.Manager, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	return router.Register("queue/{name}", jsonschema.MustFor[queueName](), func(path httprequest.PathItem) {
		path.Tag("Queue")

		// GET
		path.Get(func(w http.ResponseWriter, r *http.Request) {
			if queue, err := manager.GetQueue(r.Context(), r.PathValue("name")); err != nil {
				httpresponse.Error(w, httpErr(err), r.PathValue("name"))
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), queue)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Get Queue")
			op.Description(documentation.Section(3, "GET /queue/{name}").Body)
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.QueueWithStatus](), "Queue, with the number of its tasks with each status")
			op.ErrorResponse(http.StatusBadRequest, "Invalid queue name")
			op.ErrorResponse(http.StatusNotFound, "Queue not found")
		})

		// PATCH
		path.Patch(func(w http.ResponseWriter, r *http.Request) {
			var meta schema.QueueMeta
			if err := httprequest.Read(r, &meta); err != nil {
				httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
			} else if queue, err := manager.UpdateQueue(r.Context(), r.PathValue("name"), meta); err != nil {
				httpresponse.Error(w, httpErr(err), r.PathValue("name"))
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), queue)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Update Queue")
			op.Description(documentation.Section(3, "PATCH /queue/{name}").Body)
			op.RequestBody(jsonschema.MustFor[schema.QueueMeta]())
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Queue](), "Queue")
			op.ErrorResponse(http.StatusBadRequest, "Invalid queue name, or settings")
			op.ErrorResponse(http.StatusNotFound, "Queue not found")
		})

		// DELETE
		path.Delete(func(w http.ResponseWriter, r *http.Request) {
			if queue, err := manager.DeleteQueue(r.Context(), r.PathValue("name")); err != nil {
				httpresponse.Error(w, httpErr(err), r.PathValue("name"))
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), queue)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Delete Queue")
			op.Description(documentation.Section(3, "DELETE /queue/{name}").Body)
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Queue](), "Queue which was deleted")
			op.ErrorResponse(http.StatusBadRequest, "Invalid queue name")
			op.ErrorResponse(http.StatusNotFound, "Queue not found")
		})
	})
}

func queueCleanHandler(manager *pgqueue.Manager, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	return router.Register("queue/{name}/clean", jsonschema.MustFor[queueName](), func(path httprequest.PathItem) {
		path.Tag("Queue")

		// POST
		path.Post(func(w http.ResponseWriter, r *http.Request) {
			if tasks, err := manager.CleanQueue(r.Context(), r.PathValue("name")); err != nil {
				httpresponse.Error(w, httpErr(err), r.PathValue("name"))
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), schema.QueueCleanResponse{
					QueueCleanRequest: schema.QueueCleanRequest{Queue: r.PathValue("name")},
					Body:              tasks,
				})
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Clean Queue")
			op.Description(documentation.Section(3, "POST /queue/{name}/clean").Body)
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.QueueCleanResponse](), "Tasks which were deleted")
			op.ErrorResponse(http.StatusBadRequest, "Invalid queue name")
		})
	})
}
