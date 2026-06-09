package httphandler

import (
	"net/http"
	"strings"

	// Packages
	manager "github.com/mutablelogic/go-pg/pkg/manager"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterSchemaHandlers registers HTTP handlers for schema CRUD operations
// on the provided router with the given path prefix. The manager must be non-nil.
func RegisterSchemaHandlers(router *http.ServeMux, prefix string, manager *manager.Manager) {
	if manager == nil {
		panic("manager is nil")
	}

	// List schemas across all databases
	router.HandleFunc(joinPath(prefix, "schema"), func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = schemaList(w, r, manager, nil)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	// List schemas in a specific database, or create a new schema
	router.HandleFunc(joinPath(prefix, "schema/{database}"), func(w http.ResponseWriter, r *http.Request) {
		database := r.PathValue("database")
		if database == "" {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing or invalid database name"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			_ = schemaList(w, r, manager, &database)
		case http.MethodPost:
			_ = schemaCreate(w, r, manager, database)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	// Get, update, or delete a specific schema
	router.HandleFunc(joinPath(prefix, "schema/{database}/{namespace}"), func(w http.ResponseWriter, r *http.Request) {
		database := r.PathValue("database")
		if database == "" {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing or invalid database name"))
			return
		}
		namespace := r.PathValue("namespace")
		if namespace == "" {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing or invalid schema name"))
			return
		}
		if strings.HasPrefix(namespace, "pg_") {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("schema name cannot start with reserved prefix 'pg_'"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			_ = schemaGet(w, r, manager, database, namespace)
		case http.MethodPatch:
			_ = schemaUpdate(w, r, manager, database, namespace)
		case http.MethodDelete:
			_ = schemaDelete(w, r, manager, database, namespace)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func schemaGet(w http.ResponseWriter, r *http.Request, manager *manager.Manager, database, namespace string) error {
	s, err := manager.GetSchema(r.Context(), database, namespace)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), s)
}

func schemaList(w http.ResponseWriter, r *http.Request, manager *manager.Manager, database *string) error {
	// Parse request
	var req schema.SchemaListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}
	req.Database = database

	// List the schemas
	response, err := manager.ListSchemas(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

func schemaCreate(w http.ResponseWriter, r *http.Request, manager *manager.Manager, database string) error {
	// Parse request
	var req schema.SchemaMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Create the schema
	response, err := manager.CreateSchema(r.Context(), database, req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusCreated, httprequest.Indent(r), response)
}

func schemaDelete(w http.ResponseWriter, r *http.Request, manager *manager.Manager, database, namespace string) error {
	// Parse the query
	var req struct {
		Force bool `json:"force,omitempty" help:"Force delete with CASCADE"`
	}
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Delete the schema
	_, err := manager.DeleteSchema(r.Context(), database, namespace, req.Force)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.Empty(w, http.StatusOK)
}

func schemaUpdate(w http.ResponseWriter, r *http.Request, manager *manager.Manager, database, namespace string) error {
	// Parse request
	var req schema.SchemaMeta
	if err := httprequest.Read(r, &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Perform update
	s, err := manager.UpdateSchema(r.Context(), database, namespace, req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), s)
}
