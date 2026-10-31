package httphandler

import (
	_ "embed"
	"errors"

	// Packages
	broadcaster "github.com/mutablelogic/go-pg/pgbroadcaster"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	openapi "github.com/mutablelogic/go-server/pkg/openapi"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed README.md
var readme []byte

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterHandlers(manager *broadcaster.Manager, router *httprouter.Router) error {
	var documentation = openapi.ParseMarkdown(readme)
	router.Spec().AddTagGroup("Events", "Events")
	return errors.Join(
		eventHandlers(manager, router, documentation),
	)
}
