package httphandlers

import (
	"errors"
	"net/http"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pgmanager/manager"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterStatusHandlers(manager *manager.Manager, router *httprouter.Router) error {
	return errors.Join(
		// Register Ping Handler
		router.RegisterPath("health", nil, httprequest.NewPathItem("Health", "Determine the health of the PostgreSQL server").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = Ping(w, r, manager)
				},
				"Ping the postgresql server",
				openapi.WithTags("Status"),
				openapi.WithNoContentResponse(http.StatusNoContent, "PostgreSQL server is healthy"),
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func Ping(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	if err := manager.Ping(r.Context()); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	}
	return httpresponse.Empty(w, http.StatusNoContent)
}
