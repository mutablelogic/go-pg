package httphandler

import (
	"net/http"

	// Packages
	"github.com/mutablelogic/go-server/pkg/httpresponse"
)

// RegisterFrontendHandler registers a fallback handler when frontend is not included
func RegisterFrontendHandler(router *http.ServeMux, prefix string, enabled bool) {
	// Catch all handler returns a "not found" error
	router.HandleFunc(joinPath(prefix, "/"), func(w http.ResponseWriter, r *http.Request) {
		_ = httpresponse.Error(w, httpresponse.ErrNotFound, r.URL.String())
	})
}
