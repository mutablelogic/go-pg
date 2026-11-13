package schema

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	tui "github.com/mutablelogic/go-server/pkg/tui"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// SettingName is a setting name identifier
type SettingName string

// SettingCategory is a top level of the settings' categories, which are two
// levels, such as "Client Connection Defaults / Locale and Formatting", with
// how many settings it has, and how many of them are changed
type SettingCategory struct {
	Name    string `json:"name"`
	Count   uint64 `json:"count"`
	Changed uint64 `json:"changed"`
}

// SettingMeta represents the mutable parts of a setting
type SettingMeta struct {
	Value *string `json:"value"`
}

// Setting represents a PostgreSQL server setting
type Setting struct {
	Name string `json:"name"`
	SettingMeta
	Unit        *string `json:"unit,omitempty"`
	Category    string  `json:"category"`
	Context     string  `json:"context"` // internal, postmaster, sighup, superuser, user
	Description string  `json:"description,omitempty"`
	ExtraDesc   string  `json:"extra_desc,omitempty"`

	// Changed is true when the setting is set for the server, such as in its
	// configuration file, or by ALTER SYSTEM, rather than its default, or
	// this connection's
	Changed bool `json:"changed,omitempty"`

	// Type is the setting's type: bool, enum, integer, real or string
	Type string `json:"type"`

	// Min and Max are the limits of an integer or real, in its unit
	Min *string `json:"min,omitempty"`
	Max *string `json:"max,omitempty"`

	// Values are the values an enum can be
	Values []string `json:"values,omitempty"`

	// Default is the value it has when it isn't set, which PostgreSQL is
	// built with, in its unit
	Default *string `json:"default,omitempty"`

	// Source is where its value is from, such as default, configuration file,
	// or command line
	Source string `json:"source"`

	// PendingRestart is true when it's changed in the configuration, but the
	// server needs to restart before it takes effect
	PendingRestart bool `json:"pending_restart,omitempty"`

	// Config is its value in the configuration files, such as
	// postgresql.conf, and Override its value set by ALTER SYSTEM, in
	// postgresql.auto.conf, which is used rather than the configuration
	// files'. They're read for a setting by its name, by a role which can
	// read the configuration files, and are in its unit, or as they're
	// written, such as 128MB.
	Config   *string `json:"config,omitempty"`
	Override *string `json:"override,omitempty"`
}

// SettingFileRequest is the values of a setting, by its name, in the
// configuration files
type SettingFileRequest string

// SettingFiles are the values of a setting in the configuration files, the
// last of which in each file is used
type SettingFiles struct {
	Config   *string
	Override *string
}

// SettingFileAccess is whether the configuration files can be read
type SettingFileAccess bool

// ConfigLoadTime is when the session last loaded the configuration files
type ConfigLoadTime time.Time

// SettingListRequest is used to retrieve server settings
type SettingListRequest struct {
	pg.OffsetLimit

	// Category is a category, such as "Client Connection Defaults / Locale and
	// Formatting", or a top level of them, such as "Client Connection
	// Defaults", which is every category under it
	Category *string `json:"category,omitempty" help:"Filter by category, or a top level of them"`

	// Sort is what the settings are sorted by, and Order its direction, asc or
	// desc, which is asc when it's empty. Empty values are last, either way.
	Sort  string `json:"sort,omitempty" help:"Sort by name, category, context or changed"`
	Order string `json:"order,omitempty" help:"Order of the sort (asc or desc)"`
}

// SettingList contains the list of settings
type SettingList struct {
	SettingListRequest
	Count uint64    `json:"count"`
	Body  []Setting `json:"body,omitempty"`
}

// SettingCategoryListRequest is used to retrieve the top levels of the
// settings' categories
type SettingCategoryListRequest struct{}

// SettingCategoryList contains the top levels of the settings' categories
type SettingCategoryList struct {
	Count uint64            `json:"count"`
	Body  []SettingCategory `json:"body,omitempty"`
}

///////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (s Setting) String() string {
	return types.Stringify(s)
}

func (s SettingList) String() string {
	return types.Stringify(s)
}

func (s SettingCategoryList) String() string {
	return types.Stringify(s)
}

////////////////////////////////////////////////////////////////////////////////
// TABLE

func (r Setting) Header() []string {
	return []string{"Name", "Value", "Unit", "Category", "Context", "Description", "ExtraDesc"}
}

func (r Setting) Width(col int) int {
	return 0
}

func (r Setting) Cell(col int) tui.CellText {
	switch col {
	case 0:
		return r.Name
	case 1:
		if r.Value == nil {
			return ""
		}
		return *r.Value
	case 2:
		if r.Unit == nil {
			return ""
		}
		return *r.Unit
	case 3:
		return r.Category
	case 4:
		return r.Context
	case 5:
		return r.Description
	case 6:
		return r.ExtraDesc
	default:
		return ""
	}
}

func (r SettingCategory) Header() []string {
	return []string{"Category", "Settings", "Changed"}
}

func (r SettingCategory) Width(col int) int {
	return 0
}

func (r SettingCategory) Cell(col int) tui.CellText {
	switch col {
	case 0:
		return r.Name
	case 1:
		return fmt.Sprint(r.Count)
	case 2:
		return fmt.Sprint(r.Changed)
	default:
		return ""
	}
}

////////////////////////////////////////////////////////////////////////////////
// QUERY

func (d SettingListRequest) Query() url.Values {
	q := url.Values{}
	if d.Offset > 0 {
		q.Set("offset", fmt.Sprint(d.Offset))
	}
	if d.Limit != nil {
		q.Set("limit", fmt.Sprint(types.Value(d.Limit)))
	}
	if d.Category != nil {
		q.Set("category", *d.Category)
	}
	if d.Sort != "" {
		q.Set("sort", d.Sort)
	}
	if d.Order != "" {
		q.Set("order", d.Order)
	}
	return q
}

func (d SettingCategoryListRequest) Query() url.Values {
	return url.Values{}
}

///////////////////////////////////////////////////////////////////////////////
// SELECT

// settingSorts are what settings sort by
var settingSorts = sortColumns{
	"name":     {`"name"`},
	"category": {`"category"`},
	"context":  {`"context"`},
	"changed":  {`"changed"`},
}

func (r *SettingListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Set empty where
	bind.Set("where", "")
	orderby, err := orderBy(r.Sort, r.Order, settingSorts, `ORDER BY "category", "name"`, `"name"`)
	if err != nil {
		return "", err
	}
	bind.Set("orderby", orderby)

	// Filter by category
	if r.Category != nil {
		bind.Set("category", strings.TrimSpace(*r.Category))
		bind.Set("where", `WHERE ("category" = @category OR starts_with("category", @category || ' / '))`)
	}

	// Bind offset and limit
	r.OffsetLimit.Bind(bind, SettingListLimit)

	// Return query
	switch op {
	case pg.List:
		return settingList, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported SettingListRequest operation %q", op)
	}
}

func (r SettingCategoryListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Return query
	switch op {
	case pg.List:
		return settingCategoryList, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported SettingCategoryListRequest operation %q", op)
	}
}

func (r SettingFileRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	bind.Set("name", string(r))
	switch op {
	case pg.List:
		return settingFileList, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported SettingFileRequest operation %q", op)
	}
}

func (SettingFileAccess) Select(bind *pg.Bind, op pg.Op) (string, error) {
	switch op {
	case pg.Get:
		return settingFileAccess, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported SettingFileAccess operation %q", op)
	}
}

func (n SettingName) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Set name
	bind.Set("name", string(n))

	// Return query
	switch op {
	case pg.Get:
		return settingGet, nil
	case pg.Update:
		// Check for reset (nil value)
		if value := bind.Get("value"); value == nil {
			return settingReset, nil
		}
		return settingUpdate, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported SettingName operation %q", op)
	}
}

///////////////////////////////////////////////////////////////////////////////
// WRITER

// Insert is not supported for settings - they cannot be created, only updated.
func (m SettingMeta) Insert(_ *pg.Bind) (string, error) {
	return "", pg.ErrNotImplemented.With("settings cannot be inserted")
}

func (m SettingMeta) Update(bind *pg.Bind) error {
	if m.Value != nil {
		bind.Set("value", types.Value(m.Value))
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// READER

func (s *Setting) Scan(row pg.Row) error {
	return row.Scan(&s.Name, &s.Value, &s.Unit, &s.Category, &s.Context, &s.Description, &s.ExtraDesc, &s.Changed, &s.Type, &s.Min, &s.Max, &s.Values, &s.Default, &s.Source, &s.PendingRestart)
}

// Scan is a value in a configuration file: postgresql.auto.conf's are the
// override, and the others' the configuration's, and the last of each is
// used
func (f *SettingFiles) Scan(row pg.Row) error {
	var file, value string
	if err := row.Scan(&file, &value); err != nil {
		return err
	}
	if filepath.Base(file) == "postgresql.auto.conf" {
		f.Override = types.Ptr(value)
	} else {
		f.Config = types.Ptr(value)
	}
	return nil
}

func (t *ConfigLoadTime) Scan(row pg.Row) error {
	return row.Scan((*time.Time)(t))
}

func (ConfigLoadTime) Select(bind *pg.Bind, op pg.Op) (string, error) {
	switch op {
	case pg.Get:
		return `SELECT pg_conf_load_time()`, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported ConfigLoadTime operation %q", op)
	}
}

func (a *SettingFileAccess) Scan(row pg.Row) error {
	return row.Scan((*bool)(a))
}

func (l *SettingList) Scan(row pg.Row) error {
	var setting Setting
	if err := setting.Scan(row); err != nil {
		return err
	}
	l.Body = append(l.Body, setting)
	return nil
}

func (l *SettingList) ScanCount(row pg.Row) error {
	return row.Scan(&l.Count)
}

func (l *SettingCategoryList) Scan(row pg.Row) error {
	var category SettingCategory
	if err := row.Scan(&category.Name, &category.Count, &category.Changed); err != nil {
		return err
	}
	l.Body = append(l.Body, category)
	return nil
}

func (l *SettingCategoryList) ScanCount(row pg.Row) error {
	return row.Scan(&l.Count)
}

///////////////////////////////////////////////////////////////////////////////
// SQL

// settingChanged is true when a setting is set for the server, such as in
// its configuration file, or by ALTER SYSTEM, rather than its default, or
// the connection's
const settingChanged = `source NOT IN ('default', 'override', 'client', 'session')`

const (
	settingSelect = `
		SELECT
			name AS "name",
			setting AS "value",
			unit AS "unit",
			category AS "category",
			context AS "context",
			COALESCE(short_desc, '') AS "description",
			COALESCE(extra_desc, '') AS "extra_desc",
			` + settingChanged + ` AS "changed",
			vartype AS "type",
			min_val AS "min",
			max_val AS "max",
			enumvals AS "values",
			boot_val AS "default",
			source AS "source",
			pending_restart AS "pending_restart"
		FROM
			pg_catalog.pg_settings
	`
	settingList   = `WITH q AS (` + settingSelect + `) SELECT * FROM q ${where} ${orderby}`
	settingGet    = settingSelect + ` WHERE name = ${'name'}`
	settingUpdate = `ALTER SYSTEM SET ${"name"} = ${'value'}`
	settingReset  = `ALTER SYSTEM RESET ${"name"}`

	// settingFileList are a setting's values in the configuration files, in
	// the order they're read, without those which are in error, and
	// settingFileAccess is whether they can be read, which needs the view,
	// and the function it calls
	settingFileList     = `SELECT sourcefile, setting FROM pg_catalog.pg_file_settings WHERE lower(name) = lower(@name) AND error IS NULL AND setting IS NOT NULL ORDER BY seqno`
	settingFileAccess   = `SELECT has_function_privilege('pg_catalog.pg_show_all_file_settings()', 'EXECUTE') AND has_table_privilege('pg_catalog.pg_file_settings', 'SELECT')`
	settingCategoryList = `
		SELECT
			split_part(category, ' / ', 1) AS "name",
			COUNT(*) AS "count",
			COUNT(*) FILTER (WHERE ` + settingChanged + `) AS "changed"
		FROM
			pg_catalog.pg_settings
		GROUP BY
			1
		ORDER BY
			1
	`
)
