package httphandler

import (
	"errors"
	"net/http"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterBackendHandlers registers all queue HTTP handlers on the provided
// router with the given path prefix. The manager must be non-nil.
func RegisterBackendHandlers(router *http.ServeMux, prefix string, manager *queue.Manager) {
	RegisterQueueHandlers(router, prefix, manager)
	RegisterTaskHandlers(router, prefix, manager)
	RegisterTickerHandlers(router, prefix, manager)
	RegisterNamespaceHandlers(router, prefix, manager)
	RegisterMetricsHandler(router, prefix, manager)
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func joinPath(prefix, path string) string {
	return types.JoinPath(prefix, path)
}

// httperr converts pg errors to appropriate HTTP errors.
// Returns the original error if it's already an httpresponse.Err,
// otherwise maps pg errors to their HTTP equivalents.
func httperr(err error) error {
	if err == nil {
		return nil
	}

	// If already an HTTP error, return as-is
	var httpErr httpresponse.Err
	if errors.As(err, &httpErr) {
		return err
	}

	// Map pg errors to HTTP errors
	switch {
	case errors.Is(err, pg.ErrNotFound):
		return httpresponse.ErrNotFound.With(err.Error())
	case errors.Is(err, pg.ErrBadParameter):
		return httpresponse.ErrBadRequest.With(err.Error())
	case errors.Is(err, pg.ErrNotImplemented):
		return httpresponse.ErrNotImplemented.With(err.Error())
	case errors.Is(err, pg.ErrNotAvailable):
		return httpresponse.ErrNotImplemented.With(err.Error())
	default:
		return httpresponse.ErrInternalError.With(err.Error())
	}
}
