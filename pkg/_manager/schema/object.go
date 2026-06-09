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

type ObjectName struct {
	Schema string `json:"schema,omitempty" help:"Schema"`
	Name   string `json:"name,omitempty" arg:"" help:"Name"`
}

type ObjectMeta struct {
	Name  string  `json:"name,omitempty" arg:"" help:"Name"`
	Owner string  `json:"owner,omitempty" help:"Owner"`
	Acl   ACLList `json:"acl,omitempty" help:"Access privileges"`
}

// TableMeta contains metadata specific to tables
type TableMeta struct {
	LiveTuples *int64 `json:"live_tuples,omitempty" help:"Number of live tuples"`
	DeadTuples *int64 `json:"dead_tuples,omitempty" help:"Number of dead tuples"`
}

type Object struct {
	Oid      uint32 `json:"oid"`
	Database string `json:"database,omitempty" help:"Database"`
	Schema   string `json:"schema,omitempty" help:"Schema"`
	Type     string `json:"type,omitempty" help:"Type"`
	ObjectMeta
	Tablespace *string    `json:"tablespace,omitempty" help:"Tablespace"`
	Size       uint64     `json:"bytes,omitempty" help:"Size of object in bytes"`
	Table      *TableMeta `json:"table,omitempty" help:"Table-specific metadata"`
}

type ObjectListRequest struct {
	Database *string `json:"database,omitempty" help:"Database"`
	Schema   *string `json:"schema,omitempty" help:"Schema"`
	Type     *string `json:"type,omitempty" help:"Object Type"`
	pg.OffsetLimit
}

type ObjectList struct {
	Count uint64   `json:"count"`
	Body  []Object `json:"body,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (o ObjectMeta) String() string {
	data, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (t TableMeta) String() string {
	data, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (o Object) String() string {
	data, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (o ObjectListRequest) String() string {
	data, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (o ObjectList) String() string {
	data, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (o ObjectName) String() string {
	data, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

////////////////////////////////////////////////////////////////////////////////
// SELECT

func (o ObjectName) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Validate and set schema
	if schema, err := o.schema(); err != nil {
		return "", err
	} else {
		bind.Set("schema", schema)
	}

	// Validate and set name
	if name, err := o.name(); err != nil {
		return "", err
	} else {
		bind.Set("name", name)
	}

	// Return query
	switch op {
	case pg.Get:
		return objectGet, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported ObjectName operation %q", op)
	}
}

func (o ObjectListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Order
	bind.Set("orderby", `ORDER BY schema ASC, name ASC`)

	// Where
	bind.Del("where")
	if o.Schema != nil {
		if schema := strings.TrimSpace(*o.Schema); schema != "" {
			bind.Append("where", `schema = `+types.Quote(schema))
		}
	}
	if o.Database != nil {
		if database := strings.TrimSpace(*o.Database); database != "" {
			bind.Append("where", `database = `+types.Quote(database))
		}
	}
	if o.Type != nil {
		if objectType := strings.TrimSpace(*o.Type); objectType != "" {
			bind.Append("where", `type = `+types.Quote(objectType))
		}
	}
	if where := bind.Join("where", " AND "); where != "" {
		bind.Set("where", `WHERE `+where)
	} else {
		bind.Set("where", "")
	}

	// Bind offset and limit
	o.OffsetLimit.Bind(bind, ObjectListLimit)

	// Return query
	switch op {
	case pg.List:
		return objectList, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported ObjectListRequest operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (o *Object) Scan(row pg.Row) error {
	var priv []string
	var liveTuples, deadTuples *int64
	o.Acl = ACLList{}
	if err := row.Scan(&o.Oid, &o.Database, &o.Schema, &o.Name, &o.Type, &o.Owner, &priv, &o.Tablespace, &o.Size, &liveTuples, &deadTuples); err != nil {
		return err
	}
	for _, v := range priv {
		item, err := NewACLItem(v)
		if err != nil {
			return err
		}
		o.Acl.Append(item)
	}
	// Only set Table if we have tuple data (i.e., it's a table)
	if liveTuples != nil || deadTuples != nil {
		o.Table = &TableMeta{
			LiveTuples: liveTuples,
			DeadTuples: deadTuples,
		}
	}
	return nil
}

func (o *ObjectList) Scan(row pg.Row) error {
	var object Object
	if err := object.Scan(row); err != nil {
		return err
	} else {
		o.Body = append(o.Body, object)
	}
	return nil
}

func (o *ObjectList) ScanCount(row pg.Row) error {
	return row.Scan(&o.Count)
}

////////////////////////////////////////////////////////////////////////////////
// VALIDATION

// schema validates and returns the schema name.
// Returns an error if the schema is empty.
func (o ObjectName) schema() (string, error) {
	schema := strings.TrimSpace(o.Schema)
	if schema == "" {
		return "", pg.ErrBadParameter.With("schema is required")
	}
	return schema, nil
}

// name validates and returns the object name.
// Returns an error if the name is empty.
func (o ObjectName) name() (string, error) {
	name := strings.TrimSpace(o.Name)
	if name == "" {
		return "", pg.ErrBadParameter.With("name is required")
	}
	return name, nil
}

// Validate checks that the ObjectName has valid schema and name.
func (o ObjectName) Validate() error {
	if _, err := o.schema(); err != nil {
		return err
	}
	if _, err := o.name(); err != nil {
		return err
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// SQL

const (
	ObjectDef    = `object ("oid" OID, "database" TEXT, "schema" TEXT, "name" TEXT, "type" TEXT, "owner" TEXT, "acl" TEXT[], "tablespace" TEXT, "size" BIGINT, "live_tuples" BIGINT, "dead_tuples" BIGINT)`
	objectSelect = `
		WITH objects AS (
			SELECT
				C.oid AS oid,
				current_database() AS database,
				N.nspname AS schema,
				C.relname AS name,
				CASE C.relkind
					WHEN 'r' THEN 'TABLE'
					WHEN 'v' THEN 'VIEW'
					WHEN 'i' THEN 'INDEX'
					WHEN 'S' THEN 'SEQUENCE'
					WHEN 'm' THEN 'MATERIALIZED VIEW'
					WHEN 'c' THEN 'COMPOSITE TYPE'
					WHEN 't' THEN 'TOAST TABLE'
					WHEN 'f' THEN 'FOREIGN TABLE'
					WHEN 'p' THEN 'PARTITIONED TABLE'
					WHEN 'I' THEN 'PARTITIONED INDEX'
					ELSE C.relkind::TEXT
				END AS type,
				R.rolname AS owner,
				C.relacl AS acl,
				T.spcname AS tablespace,
				CASE C.relkind
					WHEN 'r' THEN pg_table_size(C.oid)
					ELSE pg_relation_size(C.oid)
				END AS size,
				S.n_live_tup AS live_tuples,
				S.n_dead_tup AS dead_tuples
			FROM
				pg_class C
			JOIN
				pg_namespace N ON N.oid = C.relnamespace
			JOIN
				pg_roles R ON R.oid = C.relowner
			LEFT JOIN
				pg_tablespace T ON T.oid = C.reltablespace
			LEFT JOIN
				pg_stat_user_tables S ON S.relid = C.oid
			WHERE
				N.nspname NOT LIKE 'pg_%' AND N.nspname != 'information_schema' AND C.relkind != 't'
		) SELECT * FROM objects
	`
	objectGet  = objectSelect + `WHERE name = ${'name'} AND schema = ${'schema'}`
	objectList = `WITH q AS (` + objectSelect + `) SELECT * FROM q ${where} ${orderby}`
)
