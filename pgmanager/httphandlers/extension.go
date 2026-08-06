package httphandlers

import (
	"errors"
	"net/http"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pgmanager/manager"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ExtensionPathParams struct {
	Extension string `json:"extension"`
}

type ExtensionDeleteQueryParams struct {
	Database []string `json:"database" query:"database" help:"Database to delete extension from."`
	Cascade  bool     `json:"cascade" query:"cascade" help:"Cascade option."`
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func RegisterExtensionHandlers(manager *manager.Manager, router *httprouter.Router) error {
	router.Spec().AddTag("Extensions", "Database Extension Operations")

	return errors.Join(
		router.RegisterPath("extension", nil, httprequest.NewPathItem("Extensions", "Manage PostgreSQL extensions").Tag("Extensions").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = ListExtensions(w, r, manager)
				},
				func(op httprequest.PathOperation) {
					op.Summary("List extensions")
					op.Query(jsonschema.MustFor[schema.ExtensionListRequest]())
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.ExtensionList]())
				},
			).
			Post(
				func(w http.ResponseWriter, r *http.Request) {
					_ = CreateExtension(w, r, manager)
				},
				func(op httprequest.PathOperation) {
					op.Summary("Install extension into a database")
					op.RequestBody(jsonschema.MustFor[schema.ExtensionMeta]())
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Extension]())
				},
			),
		),
		router.RegisterPath("extension/{extension}", jsonschema.MustFor[ExtensionPathParams](), httprequest.NewPathItem("Extension", "Manage a PostgreSQL extension").Tag("Extensions").
			Get(
				func(w http.ResponseWriter, r *http.Request) {
					_ = GetExtension(w, r, manager, r.PathValue("extension"))
				},
				func(op httprequest.PathOperation) {
					op.Summary("Get extension")
					op.JSONResponse(http.StatusOK, jsonschema.MustFor[schema.Extension]())
				},
			).
			Delete(
				func(w http.ResponseWriter, r *http.Request) {
					_ = DeleteExtension(w, r, manager, r.PathValue("extension"))
				},
				func(op httprequest.PathOperation) {
					op.Summary("Uninstall extension from one or more databases")
					op.Query(jsonschema.MustFor[ExtensionDeleteQueryParams]())
					op.Description("Delete an extension from the specified databases")
				},
			),
		),
	)
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func ListExtensions(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var req schema.ExtensionListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	if extensions, err := manager.ListExtensions(r.Context(), req); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), extensions)
	}
}

func GetExtension(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	if extension, err := manager.GetExtension(r.Context(), name); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), extension)
	}
}

func CreateExtension(w http.ResponseWriter, r *http.Request, manager *manager.Manager) error {
	var meta schema.ExtensionMeta
	if err := httprequest.Read(r, &meta); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}
	// TODO: Cascade option
	if extension, err := manager.CreateExtension(r.Context(), meta, false); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	} else {
		return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), extension)
	}
}

func DeleteExtension(w http.ResponseWriter, r *http.Request, manager *manager.Manager, name string) error {
	var query ExtensionDeleteQueryParams
	if err := httprequest.Query(r.URL.Query(), &query); err != nil {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With(err.Error()))
	}

	// Check extension existence
	if _, err := manager.GetExtension(r.Context(), name); err != nil {
		return httpresponse.Error(w, pg.HTTPError(err))
	}

	// If no databases specified, return bad request
	if len(query.Database) == 0 {
		return httpresponse.Error(w, httpresponse.ErrBadRequest.With("at least one database must be specified for extension deletion"))
	}

	// Check to make sure extension is in all the databases first
	for _, database := range query.Database {
		if _, err := manager.GetInstalledExtension(r.Context(), name, database); err != nil {
			return httpresponse.Error(w, pg.HTTPError(err))
		}
	}

	// Now we have a good chance of successfully deleting the extension in each database
	for _, database := range query.Database {
		if err := manager.DeleteExtension(r.Context(), database, name, query.Cascade); err != nil {
			return httpresponse.Error(w, pg.HTTPError(err))
		}
	}

	// Return success
	return httpresponse.Empty(w, http.StatusNoContent)
}
