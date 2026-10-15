package httphandler

import (
	_ "embed"
	"errors"

	// Packages
	pgqueue "github.com/mutablelogic/go-pg/pgqueue"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed README.md
var readme []byte

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// RegisterHandlers registers the HTTP handlers for the queues, tasks and
// tickers of the manager with the router
func RegisterHandlers(manager *pgqueue.Manager, router *httprouter.Router) error {
	var documentation = openapi.ParseMarkdown(readme)
	router.Spec().AddTag("Queue", documentation.Section(2, "Queue Operations").Body)
	router.Spec().AddTag("Ticker", documentation.Section(2, "Ticker Operations").Body)
	router.Spec().AddTag("Task", documentation.Section(2, "Task Operations").Body)
	router.Spec().AddTagGroup("Task Queue", "Queue", "Ticker", "Task")
	return errors.Join(
		queueHandlers(manager, router, documentation),
		tickerHandlers(manager, router, documentation),
		taskHandlers(manager, router, documentation),
	)
}
