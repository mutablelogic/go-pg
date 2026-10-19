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

// tickerName is the name of a ticker in the path
type tickerName struct {
	Name string `json:"name" help:"Ticker name"`
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func tickerHandlers(manager *pgqueue.Manager, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	return errors.Join(
		tickerListHandler(manager, router, documentation),
		tickerGetHandler(manager, router, documentation),
	)
}

func tickerListHandler(manager *pgqueue.Manager, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	return router.Register("ticker", nil, func(path httprequest.PathItem) {
		path.Tag("Ticker")

		// GET
		path.Get(func(w http.ResponseWriter, r *http.Request) {
			var req schema.TickerListRequest
			if err := httprequest.Query(r.URL.Query(), &req); err != nil {
				httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
			} else if list, err := manager.ListTickers(r.Context(), req); err != nil {
				httpresponse.Error(w, httpErr(err))
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), list)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("List Tickers")
			op.Description(documentation.Section(3, "GET /ticker").Body)
			op.Query(jsonschema.MustFor[schema.TickerListRequest]())
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.TickerList](), "List of tickers")
			op.ErrorResponse(http.StatusBadRequest, "Invalid request")
		})
	})
}

func tickerGetHandler(manager *pgqueue.Manager, router *httprouter.Router, documentation *openapi.MarkdownDoc) error {
	return router.Register("ticker/{name}", jsonschema.MustFor[tickerName](), func(path httprequest.PathItem) {
		path.Tag("Ticker")

		// GET
		path.Get(func(w http.ResponseWriter, r *http.Request) {
			if ticker, err := manager.GetTicker(r.Context(), r.PathValue("name")); err != nil {
				httpresponse.Error(w, httpErr(err), r.PathValue("name"))
			} else {
				httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), ticker)
			}
		}, func(op httprequest.PathOperation) {
			op.Summary("Get Ticker")
			op.Description(documentation.Section(3, "GET /ticker/{name}").Body)
			op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Ticker](), "Ticker")
			op.ErrorResponse(http.StatusBadRequest, "Invalid ticker name")
			op.ErrorResponse(http.StatusNotFound, "Ticker not found")
		})
	})
}
