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
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterStatusHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Status", "Cluster Status Operations")

	return errors.Join(
		// Register Ping Handler
		router.RegisterPath("health", nil, httprequest.NewPathItem("Health", "Determine the health of the PostgreSQL server").Tag("Status").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = Ping(w, r, manager)
				},
				func(op httprequest.PathOperation) {
					op.Summary("Ping the postgresql server")
				},
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
