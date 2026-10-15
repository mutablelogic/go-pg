package httphandler

import (
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
// PRIVATE METHODS

func taskHandlers(manager *pgqueue.Manager, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	return router.Register("task", nil, func(path httprequest.PathItem) {
		path.Tag("Task")

		// GET
		path.Get(func(w http.ResponseWriter, r *http.Request) {
			var req schema.TaskListRequest
			if err := httprequest.Query(r.URL.Query(), &req); err != nil {
				httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
			} else if list, err := manager.ListTasks(r.Context(), req); err != nil {
				httpresponse.Error(w, httpErr(err))
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), list)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("List Tasks")
			op.Description(documentation.Section(3, "GET /task").Body)
			op.Query(jsonschema.MustFor[schema.TaskListRequest]())
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.TaskList](), "List of tasks")
			op.ErrorResponse(http.StatusBadRequest, "Invalid request")
		})
	})
}
