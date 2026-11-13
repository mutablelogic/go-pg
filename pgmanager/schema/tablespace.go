package schema

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	tui "github.com/mutablelogic/go-server/pkg/tui"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type TablespaceName string

type TablespaceMeta struct {
	Name    string  `json:"name,omitempty" arg:"" help:"Name"`
	Owner   string  `json:"owner,omitempty" help:"Owner"`
	Acl     ACLList `json:"acl,omitempty" help:"Access privileges"`
	Comment *string `json:"comment,omitempty" help:"Comment, which an empty one removes"`
}

type Tablespace struct {
	Oid uint32 `json:"oid"`
	TablespaceMeta
	Location string   `json:"location,omitempty" help:"Location"`
	Options  []string `json:"options,omitempty" help:"Options"`
	Size     uint64   `json:"bytes,omitempty" help:"Size of schema in bytes"`

	// Databases are those with files in it, as their default, or for some
	// of their tables, or indexes, which it can't be deleted while it has
	Databases []string `json:"databases,omitempty" help:"Databases with files in the tablespace"`
}

type TablespaceListRequest struct {
	pg.OffsetLimit

	// Sort is what the tablespaces are sorted by, and Order its direction, asc or
	// desc, which is asc when it's empty. Empty values are last, either way.
	Sort  string `json:"sort,omitempty" help:"Sort by name, owner, location or size"`
	Order string `json:"order,omitempty" help:"Order of the sort (asc or desc)"`
}

type TablespaceList struct {
	TablespaceListRequest
	Count uint64       `json:"count"`
	Body  []Tablespace `json:"body,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (t TablespaceMeta) String() string {
	return types.Stringify(t)
}

func (t Tablespace) String() string {
	return types.Stringify(t)
}

func (t TablespaceListRequest) String() string {
	return types.Stringify(t)
}

func (t TablespaceList) String() string {
	return types.Stringify(t)
}

////////////////////////////////////////////////////////////////////////////////
// TABLE

func (r Tablespace) Header() []string {
	return []string{"Oid", "Name", "Owner", "Acl", "Location", "Options", "Size"}
}

func (r Tablespace) Width(col int) int {
	return 0
}

func (r Tablespace) Cell(col int) tui.CellText {
	switch col {
	case 0:
		return fmt.Sprint(r.Oid)
	case 1:
		return r.Name
	case 2:
		return r.Owner
	case 3:
		if len(r.Acl) == 0 {
			return ""
		}
		return fmt.Sprint(r.Acl)
	case 4:
		return r.Location
	case 5:
		if len(r.Options) == 0 {
			return ""
		}
		return fmt.Sprint(r.Options)
	case 6:
		return fmt.Sprint(r.Size)
	default:
		return ""
	}
}

////////////////////////////////////////////////////////////////////////////////
// QUERY

func (d TablespaceListRequest) Query() url.Values {
	q := url.Values{}
	if d.Offset > 0 {
		q.Set("offset", fmt.Sprint(d.Offset))
	}
	if d.Limit != nil {
		q.Set("limit", fmt.Sprint(types.Value(d.Limit)))
	}
	if d.Sort != "" {
		q.Set("sort", d.Sort)
	}
	if d.Order != "" {
		q.Set("order", d.Order)
	}
	return q
}

////////////////////////////////////////////////////////////////////////////////
// SELECT

// tablespaceSorts are what tablespaces sort by
var tablespaceSorts = sortColumns{
	"name":     {`"name"`},
	"owner":    {`"owner"`},
	"location": {`NULLIF("location", '')`},
	"size":     {`"size"`},
}

func (t *TablespaceListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Order
	orderby, err := orderBy(t.Sort, t.Order, tablespaceSorts, `ORDER BY "name" ASC`, `"name"`)
	if err != nil {
		return "", err
	}
	bind.Set("orderby", orderby)

	// Bind offset and limit
	t.OffsetLimit.Bind(bind, TablespaceListLimit)

	// Where
	bind.Set("where", ``)

	// Return query
	switch op {
	case pg.List:
		return tablespaceList, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported TablespaceListRequest operation %q", op)
	}
}

func (t TablespaceName) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Set name
	if name := strings.TrimSpace(string(t)); name == "" {
		return "", pg.ErrBadParameter.With("tablespace name is missing")
	} else {
		bind.Set("name", name)
	}

	// Return query
	switch op {
	case pg.Get:
		return tablespaceGet, nil
	case pg.Update:
		return tablespaceRename, nil
	case pg.Delete:
		return tablespaceDelete, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported TablespaceName operation %q", op)
	}
}

func (t TablespaceMeta) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Set name
	if name := strings.TrimSpace(t.Name); name == "" {
		return "", pg.ErrBadParameter.With("tablespace name is missing")
	} else {
		bind.Set("name", name)
	}

	// Return query
	switch op {
	case pg.Update:
		return tablespaceUpdate, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported TablespaceMeta operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (t *Tablespace) Scan(row pg.Row) error {
	var priv []string
	t.Acl = ACLList{}
	if err := row.Scan(&t.Oid, &t.Name, &t.Owner, &priv, &t.Options, &t.Size, &t.Location, &t.Comment, &t.Databases); err != nil {
		return err
	}
	for _, v := range priv {
		item, err := NewACLItem(v)
		if err != nil {
			return err
		}
		t.Acl.Append(item)
	}
	return nil
}

func (t *TablespaceList) Scan(row pg.Row) error {
	var tablespace Tablespace
	if err := tablespace.Scan(row); err != nil {
		return err
	} else {
		t.Body = append(t.Body, tablespace)
	}
	return nil
}

func (t *TablespaceList) ScanCount(row pg.Row) error {
	return row.Scan(&t.Count)
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

func (t TablespaceMeta) Insert(bind *pg.Bind) (string, error) {
	// Set name
	if name := strings.TrimSpace(t.Name); name == "" {
		return "", pg.ErrBadParameter.With("name is missing")
	} else if strings.HasPrefix(name, reservedPrefix) {
		return "", pg.ErrBadParameter.Withf("cannot create a tablespace prefixed with %q", reservedPrefix)
	} else {
		bind.Set("name", name)
	}

	// Set location
	if location, ok := bind.Get("location").(string); !ok {
		return "", pg.ErrBadParameter.With("location is missing")
	} else if location = strings.TrimSpace(location); location == "" {
		return "", pg.ErrBadParameter.With("location is missing")
	} else if !filepath.IsAbs(location) {
		return "", pg.ErrBadParameter.Withf("location %q is not absolute", location)
	} else {
		bind.Set("location", location)
	}

	// Set with
	if with, err := t.with(true); err != nil {
		return "", err
	} else {
		bind.Set("with", with)
	}

	// Return success
	return tablespaceCreate, nil
}

func (t TablespaceMeta) Update(bind *pg.Bind) error {
	// With
	if with, err := t.with(false); err != nil {
		return err
	} else {
		bind.Set("with", with)
	}

	// Return success
	return nil
}

func (t TablespaceName) Insert(bind *pg.Bind) (string, error) {
	return "", pg.ErrNotImplemented.With("TablespaceName.Insert")
}

func (t TablespaceName) Update(bind *pg.Bind) error {
	if name := strings.TrimSpace(string(t)); name == "" {
		return pg.ErrBadParameter.With("name is missing")
	} else if strings.HasPrefix(name, reservedPrefix) {
		return pg.ErrBadParameter.Withf("cannot rename a tablespace prefixed with %q", reservedPrefix)
	} else {
		bind.Set("old_name", name)
	}

	// Return success
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (t TablespaceMeta) with(insert bool) (string, error) {
	var with []string

	// Owner
	if owner := strings.TrimSpace(t.Owner); owner != "" {
		if insert {
			with = append(with, `OWNER `+types.DoubleQuote(owner))
		} else {
			with = append(with, `OWNER TO `+types.DoubleQuote(owner))
		}
	}

	if len(with) > 0 {
		return strings.Join(with, " "), nil
	} else {
		return "", nil
	}
}

////////////////////////////////////////////////////////////////////////////////
// SQL

const (
	tablespaceSelect = `
		WITH t AS (
			SELECT
				T.oid AS "oid", T.spcname AS "name", R.rolname AS "owner", COALESCE(T.spcacl, acldefault('t', T.spcowner)) AS "acl",  T.spcoptions AS "options", pg_tablespace_size(T.oid) AS "size",
				CASE
					WHEN T.spcname = 'pg_global' THEN (SELECT setting||'/global' FROM pg_settings WHERE name='data_directory')
					WHEN T.spcname = 'pg_default' THEN (SELECT setting||'/base' FROM pg_settings WHERE name='data_directory')
					ELSE pg_tablespace_location(T.oid)
				END AS "location",
				shobj_description(T.oid, 'pg_tablespace') AS "comment",
				ARRAY(SELECT D.datname FROM "pg_catalog"."pg_database" D WHERE D.oid IN (SELECT pg_tablespace_databases(T.oid)) ORDER BY D.datname) AS "databases"
			FROM
				"pg_catalog"."pg_tablespace" T
			JOIN
				"pg_catalog"."pg_roles" R ON T.spcowner = R.oid			
		) SELECT * FROM t`
	tablespaceGet    = tablespaceSelect + ` WHERE "name" = ${'name'}`
	tablespaceList   = `WITH q AS (` + tablespaceSelect + `) SELECT * FROM q ${where} ${orderby}`
	tablespaceCreate = `CREATE TABLESPACE ${"name"} ${with} LOCATION ${'location'}`
	tablespaceRename = `ALTER TABLESPACE ${"old_name"} RENAME TO ${"name"}`
	tablespaceUpdate = `ALTER TABLESPACE ${"name"} ${with}`
	tablespaceDelete = `DROP TABLESPACE ${"name"}`
)
