package schema

import (

	// Packages
	"fmt"
	"net/url"

	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// SettingName is a setting name identifier
type SettingName string

// CategoryName is a setting category identifier
type CategoryName string

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
}

// SettingListRequest is used to retrieve server settings
type SettingListRequest struct {
	pg.OffsetLimit
	Category *string `json:"category,omitempty" help:"Filter by category"`
}

// SettingList contains the list of settings
type SettingList struct {
	SettingListRequest
	Count uint64    `json:"count"`
	Body  []Setting `json:"body,omitempty"`
}

// SettingCategoryListRequest is used to retrieve distinct setting categories
type SettingCategoryListRequest struct{}

// SettingCategoryList contains the list of setting categories
type SettingCategoryList struct {
	Count uint64   `json:"count"`
	Body  []string `json:"body,omitempty"`
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

func (r Setting) Cell(col int) string {
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

func (r CategoryName) Header() []string {
	return []string{"Category"}
}

func (r CategoryName) Width(col int) int {
	return 0
}

func (r CategoryName) Cell(col int) string {
	switch col {
	case 0:
		return string(r)
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
	return q
}

func (d SettingCategoryListRequest) Query() url.Values {
	return url.Values{}
}

///////////////////////////////////////////////////////////////////////////////
// SELECT

func (r SettingListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Set empty where
	bind.Set("where", "")
	bind.Set("orderby", "ORDER BY category, name")

	// Filter by category
	if r.Category != nil {
		bind.Set("category", *r.Category)
		bind.Set("where", `WHERE category = @category`)
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
	// Set value (nil means reset)
	if m.Value != nil {
		bind.Set("value", *m.Value)
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// READER

func (s *Setting) Scan(row pg.Row) error {
	return row.Scan(&s.Name, &s.Value, &s.Unit, &s.Category, &s.Context, &s.Description, &s.ExtraDesc)
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
	var category string
	if err := row.Scan(&category); err != nil {
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

const (
	settingSelect = `
		SELECT
			name AS "name",
			setting AS "value",
			unit AS "unit",
			category AS "category",
			context AS "context",
			COALESCE(short_desc, '') AS "description",
			COALESCE(extra_desc, '') AS "extra_desc"
		FROM
			pg_catalog.pg_settings
	`
	settingList         = `WITH q AS (` + settingSelect + `) SELECT * FROM q ${where} ${orderby}`
	settingGet          = settingSelect + ` WHERE name = ${'name'}`
	settingUpdate       = `ALTER SYSTEM SET ${"name"} = ${'value'}`
	settingReset        = `ALTER SYSTEM RESET ${"name"}`
	settingCategoryList = `SELECT DISTINCT category FROM pg_catalog.pg_settings ORDER BY category`
)
