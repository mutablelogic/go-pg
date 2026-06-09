//go:build frontend

//go:generate sh -c "wasmbuild build -o frontend ../../../wasm/pgmanager && mv frontend/wasm_exec.html frontend/index.html"

package httphandler

import (
	"embed"
	"io/fs"
	"net/http"

	// Packages
	"github.com/mutablelogic/go-server/pkg/httpresponse"
)

//go:embed frontend/*
var frontendFS embed.FS

// RegisterFrontendHandler registers the frontend static file handler if enabled,
// otherwise registers a fallback handler that returns a not found error.
func RegisterFrontendHandler(router *http.ServeMux, prefix string, enabled bool) {
	if !enabled {
		// Fallback handler returns a "not found" error
		router.HandleFunc(joinPath(prefix, "/"), func(w http.ResponseWriter, r *http.Request) {
			_ = httpresponse.Error(w, httpresponse.ErrNotFound, r.URL.String())
		})
		return
	}

	// Get the subdirectory to strip the "frontend" prefix
	subFS, err := fs.Sub(frontendFS, "frontend")
	if err != nil {
		panic(err)
	}

	// Serve static files from the embedded frontend folder
	router.Handle(joinPath(prefix, "/"), http.StripPrefix(prefix, http.FileServer(http.FS(subFS))))
}
