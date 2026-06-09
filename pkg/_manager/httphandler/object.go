package httphandler

import (
	"net/http"

	// Packages
	manager "github.com/mutablelogic/go-pg/pkg/manager"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	httprequest "github.com/mutablelogic/go-server/pkg/httprequest"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterObjectHandlers registers HTTP handlers for object listing and retrieval
// on the provided router with the given path prefix. The manager must be non-nil.
func RegisterObjectHandlers(router *http.ServeMux, prefix string, manager *manager.Manager) {
	if manager == nil {
		panic("manager is nil")
	}

	// List objects across all databases
	router.HandleFunc(joinPath(prefix, "object"), func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = objectList(w, r, manager, nil, nil, nil)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	// List objects in a specific database
	router.HandleFunc(joinPath(prefix, "object/{database}"), func(w http.ResponseWriter, r *http.Request) {
		database := r.PathValue("database")
		if database == "" {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing or invalid database name"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			_ = objectList(w, r, manager, &database, nil, nil)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	// List objects in a specific database and schema
	router.HandleFunc(joinPath(prefix, "object/{database}/{schema}"), func(w http.ResponseWriter, r *http.Request) {
		database := r.PathValue("database")
		if database == "" {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing or invalid database name"))
			return
		}
		namespace := r.PathValue("schema")
		if namespace == "" {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing or invalid schema name"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			_ = objectList(w, r, manager, &database, &namespace, nil)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})

	// Get a specific object
	router.HandleFunc(joinPath(prefix, "object/{database}/{schema}/{name}"), func(w http.ResponseWriter, r *http.Request) {
		database := r.PathValue("database")
		if database == "" {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing or invalid database name"))
			return
		}
		namespace := r.PathValue("schema")
		if namespace == "" {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing or invalid schema name"))
			return
		}
		name := r.PathValue("name")
		if name == "" {
			_ = httpresponse.Error(w, httpresponse.ErrBadRequest.With("missing or invalid object name"))
			return
		}

		switch r.Method {
		case http.MethodGet:
			_ = objectGet(w, r, manager, database, namespace, name)
		default:
			_ = httpresponse.Error(w, httpresponse.Err(http.StatusMethodNotAllowed), r.Method)
		}
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func objectList(w http.ResponseWriter, r *http.Request, manager *manager.Manager, database, namespace, objectType *string) error {
	// Parse request
	var req schema.ObjectListRequest
	if err := httprequest.Query(r.URL.Query(), &req); err != nil {
		return httpresponse.Error(w, err)
	}

	// Apply path filters
	if database != nil {
		req.Database = database
	}
	if namespace != nil {
		req.Schema = namespace
	}
	if objectType != nil {
		req.Type = objectType
	}

	// List the objects
	response, err := manager.ListObjects(r.Context(), req)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}

func objectGet(w http.ResponseWriter, r *http.Request, manager *manager.Manager, database, namespace, name string) error {
	// Get the object
	response, err := manager.GetObject(r.Context(), database, namespace, name)
	if err != nil {
		return httpresponse.Error(w, httperr(err))
	}

	// Return success
	return httpresponse.JSON(w, http.StatusOK, httprequest.Indent(r), response)
}
