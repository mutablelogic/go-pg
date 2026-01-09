package httphandler

import (
	"errors"
	"net/http"
	"sync"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterTickerHandlers registers HTTP handlers for ticker CRUD operations
// on the provided router with the given path prefix. The manager must be non-nil.
func RegisterTickerHandlers(router *http.ServeMux, prefix string, manager *queue.Manager) {
	if manager == nil {
		panic("manager is nil")
	}

	router.HandleFunc(joinPath(prefix, "ticker"), func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			// Determine the content type - default to JSON if no Accept header
			contentType, err := types.AcceptContentType(r)
			if err != nil {
				// No Accept header or invalid - default to list (JSON)
				_ = tickerList(w, r, manager)
				return
			}
			switch contentType {
			case types.ContentTypeTextStream:
				_ = tickerNext(w, r, manager)
			default:
				_ = tickerList(w, r, manager)
			}
		case http.MethodPost:
			_ = tickerCreate(w, r, manager)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	router.HandleFunc(joinPath(prefix, "ticker/{name}"), func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		switch r.Method {
		case http.MethodGet:
			_ = tickerGet(w, r, manager, name)
		case http.MethodPatch:
			_ = tickerUpdate(w, r, manager, name)
		case http.MethodDelete:
			_ = tickerDelete(w, r, manager, name)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func tickerGet(w http.ResponseWriter, r *http.Request, manager *queue.Manager, name string) error {
	ticker, err := manager.GetTicker(r.Context(), name)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), ticker)
}

func tickerList(w http.ResponseWriter, r *http.Request, manager *queue.Manager) error {
	// Parse request
	var req schema.TickerListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// List the tickers
	response, err := manager.ListTickers(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

// Pass a stream of tickers to the client until the client closes the connection
func tickerNext(w http.ResponseWriter, r *http.Request, manager *queue.Manager) error {
	// Create a new text stream
	textStream := httpresponse.NewTextStream(w)
	defer textStream.Close()

	// Create a channel to receive tickers
	ch := make(chan *schema.Ticker)
	defer close(ch)

	// Run the ticker loop in the background
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		manager.RunTickerLoop(r.Context(), manager.Namespace(), ch, schema.TickerPeriod)
	}()

	// Receive tickers from the channel and write them to the response
FOR_LOOP:
	for {
		select {
		case <-r.Context().Done():
			break FOR_LOOP
		case ticker := <-ch:
			textStream.Write("ticker", ticker)
		}
	}

	// Wait for goroutine to end
	wg.Wait()

	// Return success
	return nil
}

func tickerCreate(w http.ResponseWriter, r *http.Request, manager *queue.Manager) error {
	// Parse request
	var req schema.TickerMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// If the ticker already exists, return an error
	if _, err := manager.GetTicker(r.Context(), req.Ticker); err == nil {
		return httpresponse.Error(w, httpresponse.ErrConflict.With("ticker already exists"), req.Ticker)
	} else if !errors.Is(err, pg.ErrNotFound) {
		return httpresponse.Error(w, httperr(err))
	}

	// Register the ticker
	response, err := manager.RegisterTicker(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), response)
}

func tickerUpdate(w http.ResponseWriter, r *http.Request, manager *queue.Manager, name string) error {
	// Parse request
	var meta schema.TickerMeta
	if err := httprequest.Read(r, &meta); err != nil {
		return httpresponse.Error(w, err)
	}

	// Perform update
	ticker, err := manager.UpdateTicker(r.Context(), name, meta)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), ticker)
}

func tickerDelete(w http.ResponseWriter, r *http.Request, manager *queue.Manager, name string) error {
	ticker, err := manager.DeleteTicker(r.Context(), name)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), ticker)
}
