package schema

import (
	"encoding/json"
	"strings"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-pg/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type ExtensionName string

type ExtensionMeta struct {
	Name     string `json:"name,omitempty" arg:"" help:"Extension name"`
	Database string `json:"database,omitempty" help:"Database to install extension into"`
	Schema   string `json:"schema,omitempty" help:"Schema to install extension into"`
	Owner    string `json:"owner,omitempty"`
	Version  string `json:"version,omitempty" help:"Extension version"`
}

type Extension struct {
	Oid *uint32 `json:"oid,omitempty"`
	ExtensionMeta
	DefaultVersion   string   `json:"default_version,omitempty"`
	InstalledVersion *string  `json:"installed_version,omitempty"`
	Relocatable      *bool    `json:"relocatable,omitempty"`
	Comment          string   `json:"comment,omitempty"`
	Requires         []string `json:"requires,omitempty"`
}

type ExtensionListRequest struct {
	Database  *string `json:"database,omitempty" help:"Database"`
	Installed *bool   `json:"installed,omitempty" help:"Filter by installed status"`
	pg.OffsetLimit
}

type ExtensionList struct {
	Count uint64      `json:"count"`
	Body  []Extension `json:"body,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (e Extension) String() string {
	data, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (e ExtensionMeta) String() string {
	data, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (e ExtensionList) String() string {
	data, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (e ExtensionListRequest) String() string {
	data, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

////////////////////////////////////////////////////////////////////////////////
// SELECT

func (e ExtensionListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	if op != pg.List {
		return "", pg.ErrNotImplemented.Withf("operation %q", op)
	}

	// Filter by installed status
	if e.Installed != nil {
		if *e.Installed {
			bind.Set("where", "WHERE installed_version IS NOT NULL")
		} else {
			bind.Set("where", "WHERE installed_version IS NULL")
		}
	} else {
		bind.Set("where", "")
	}

	e.OffsetLimit.Bind(bind, ExtensionListLimit)
	bind.Set("orderby", "ORDER BY name ASC")
	return queryExtensionList, nil
}

func (e ExtensionName) Select(bind *pg.Bind, op pg.Op) (string, error) {
	name := strings.TrimSpace(string(e))
	if name == "" {
		return "", pg.ErrBadParameter.With("name is empty")
	}

	switch op {
	case pg.Get:
		bind.Set("name", name)
		return queryExtensionGet, nil
	case pg.Update:
		// Use raw name - ${"name"} will double-quote it
		bind.Set("name", name)
		// Check if this is a schema change
		if newSchema, ok := bind.Get("new_schema").(string); ok && newSchema != "" {
			return queryExtensionSetSchema, nil
		}
		return queryExtensionUpdate, nil
	case pg.Delete:
		// Use raw name - ${"name"} will double-quote it
		bind.Set("name", name)
		// Set cascade
		if cascade, ok := bind.Get("cascade").(bool); ok && cascade {
			bind.Set("cascade", "CASCADE")
		} else {
			bind.Set("cascade", "")
		}
		return queryExtensionDrop, nil
	default:
		return "", pg.ErrNotImplemented.Withf("operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// INSERT

func (e ExtensionMeta) Insert(bind *pg.Bind) (string, error) {
	name := strings.TrimSpace(e.Name)
	if name == "" {
		return "", pg.ErrBadParameter.With("name is empty")
	}
	// Use raw name - ${"name"} will double-quote it
	bind.Set("name", name)

	// Optional schema
	if schema := strings.TrimSpace(e.Schema); schema != "" {
		bind.Set("with", "WITH SCHEMA "+types.DoubleQuote(schema))
	} else {
		bind.Set("with", "")
	}

	// Optional version
	if version := strings.TrimSpace(e.Version); version != "" {
		bind.Set("version", "VERSION "+types.Quote(version))
	} else {
		bind.Set("version", "")
	}

	// Set cascade
	if cascade, ok := bind.Get("cascade").(bool); ok && cascade {
		bind.Set("cascade", "CASCADE")
	} else {
		bind.Set("cascade", "")
	}

	return queryExtensionCreate, nil
}

////////////////////////////////////////////////////////////////////////////////
// UPDATE

func (e ExtensionMeta) Update(bind *pg.Bind) error {
	name := strings.TrimSpace(e.Name)
	if name == "" {
		return pg.ErrBadParameter.With("name is empty")
	}
	// Use raw name - ${"name"} will double-quote it
	bind.Set("name", name)

	// Schema change (SET SCHEMA) - use raw value, ${"new_schema"} will quote it
	if schema := strings.TrimSpace(e.Schema); schema != "" {
		bind.Set("new_schema", schema)
	}

	// Version to update to
	if version := strings.TrimSpace(e.Version); version != "" {
		bind.Set("version", "TO "+types.Quote(version))
	} else {
		bind.Set("version", "")
	}

	return nil
}

// UpdateQuery returns the SQL for updating an extension
func (e ExtensionMeta) UpdateQuery(bind *pg.Bind) (string, error) {
	if err := e.Update(bind); err != nil {
		return "", err
	}
	return queryExtensionUpdate, nil
}

////////////////////////////////////////////////////////////////////////////////
// SCAN

func (e *Extension) Scan(row pg.Row) error {
	return row.Scan(&e.Oid, &e.Name, &e.Owner, &e.Schema,
		&e.DefaultVersion, &e.InstalledVersion, &e.Relocatable, &e.Requires, &e.Comment)
}

func (e *ExtensionList) Scan(row pg.Row) error {
	var ext Extension
	if err := ext.Scan(row); err != nil {
		return err
	}
	e.Body = append(e.Body, ext)
	return nil
}

func (e *ExtensionList) ScanCount(row pg.Row) error {
	return row.Scan(&e.Count)
}

////////////////////////////////////////////////////////////////////////////////
// SQL

const (
	ExtensionDef = `extension ("oid" OID, "name" TEXT, "owner" TEXT, "schema" TEXT, "default_version" TEXT, "installed_version" TEXT, "relocatable" BOOLEAN, "requires" TEXT[], "comment" TEXT)`

	queryExtensionSelect = `
		WITH e AS (
			SELECT
				E.oid AS "oid",
				A.name AS "name",
				COALESCE(R.rolname, '') AS "owner",
				COALESCE(N.nspname, '') AS "schema",
				A.default_version AS "default_version",
				A.installed_version AS "installed_version",
				E.extrelocatable AS "relocatable",
				COALESCE(
					ARRAY(SELECT dep.extname FROM ${"schema"}.pg_extension dep 
						JOIN ${"schema"}.pg_depend d ON d.refobjid = dep.oid 
						WHERE d.objid = E.oid AND d.deptype = 'e'),
					ARRAY[]::text[]
				) AS "requires",
				COALESCE(A.comment, '') AS "comment"
			FROM
				${"schema"}."pg_available_extensions" A
			LEFT JOIN
				${"schema"}."pg_extension" E ON A.name = E.extname
			LEFT JOIN
				${"schema"}."pg_roles" R ON E.extowner = R.oid
			LEFT JOIN
				${"schema"}."pg_namespace" N ON E.extnamespace = N.oid
		) SELECT * FROM e
	`
	queryExtensionGet  = queryExtensionSelect + ` WHERE "name" = ${'name'}`
	queryExtensionList = `WITH q AS (` + queryExtensionSelect + `) SELECT * FROM q ${where} ${orderby}`

	queryExtensionCreate    = `CREATE EXTENSION IF NOT EXISTS ${"name"} ${with} ${version} ${cascade}`
	queryExtensionDrop      = `DROP EXTENSION ${"name"} ${cascade}`
	queryExtensionUpdate    = `ALTER EXTENSION ${"name"} UPDATE ${version}`
	queryExtensionSetSchema = `ALTER EXTENSION ${"name"} SET SCHEMA ${"new_schema"}`
)
