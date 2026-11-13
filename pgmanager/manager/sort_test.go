package manager_test

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	harness "github.com/mutablelogic/go-pg/pgmanager/test"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// Each list sorts by each of its sorts, both ways, which PostgreSQL runs
func TestListSorts(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	for _, order := range []string{"asc", "desc"} {
		for _, sort := range []string{"name", "conlimit", "expires", "login"} {
			_, err := mgr.ListRoles(ctx, schema.RoleListRequest{Sort: sort, Order: order})
			assert.NoError(t, err, "role %s %s", sort, order)
		}
		for _, sort := range []string{"name", "category", "context", "changed"} {
			_, err := mgr.ListSettings(ctx, schema.SettingListRequest{Sort: sort, Order: order})
			assert.NoError(t, err, "setting %s %s", sort, order)
		}
		for _, sort := range []string{"name", "owner", "location", "size"} {
			_, err := mgr.ListTablespaces(ctx, schema.TablespaceListRequest{Sort: sort, Order: order})
			assert.NoError(t, err, "tablespace %s %s", sort, order)
		}
		for _, sort := range []string{"name", "type", "database", "plugin", "status", "lag"} {
			_, err := mgr.ListReplicationSlots(ctx, schema.ReplicationSlotListRequest{Sort: sort, Order: order})
			assert.NoError(t, err, "replication slot %s %s", sort, order)
		}
		for _, sort := range []string{"name", "version", "default_version", "schema"} {
			_, err := mgr.ListExtensions(ctx, schema.ExtensionListRequest{Sort: sort, Order: order})
			assert.NoError(t, err, "extension %s %s", sort, order)
			_, err = mgr.ListExtensions(ctx, schema.ExtensionListRequest{Database: types.Ptr("postgres"), Installed: types.Ptr(true), Sort: sort, Order: order})
			assert.NoError(t, err, "extension in a database %s %s", sort, order)
		}
	}
}

// Roles without a connection limit are last, either way
func TestRoleSortEmptyLast(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	for name, limit := range map[string]int64{"sort_role_a": 5, "sort_role_b": 10, "sort_role_c": -1} {
		_, err := mgr.CreateRole(ctx, schema.RoleMeta{Name: name, ConnectionLimit: types.Ptr(limit)})
		require.NoError(t, err)
		defer func() { _, _ = mgr.DeleteRole(ctx, name) }()
	}
	names := func(order string) []string {
		list, err := mgr.ListRoles(ctx, schema.RoleListRequest{Sort: "conlimit", Order: order})
		require.NoError(t, err)
		var names []string
		for _, role := range list.Body {
			if strings.HasPrefix(role.Name, "sort_role_") {
				names = append(names, role.Name)
			}
		}
		return names
	}
	assert.Equal(t, []string{"sort_role_a", "sort_role_b", "sort_role_c"}, names("asc"))
	assert.Equal(t, []string{"sort_role_b", "sort_role_a", "sort_role_c"}, names("desc"))
}

// Versions which are numbers sort as numbers, so 1.10 is after 1.9
func TestExtensionSortVersion(t *testing.T) {
	mgr, ctx := harness.Begin(t)
	defer harness.End(t)

	list, err := mgr.ListExtensions(ctx, schema.ExtensionListRequest{Sort: "default_version"})
	require.NoError(t, err)
	var versions [][]int
	for _, ext := range list.Body {
		var version []int
		for _, part := range strings.Split(ext.DefaultVersion, ".") {
			n, err := strconv.Atoi(part)
			if err != nil {
				version = nil
				break
			}
			version = append(version, n)
		}
		if version != nil {
			versions = append(versions, version)
		}
	}
	require.NotEmpty(t, versions)
	assert.True(t, slices.IsSortedFunc(versions, slices.Compare[[]int]))
}

// Versions sort as numbers, of any size, and versions which aren't numbers
// are after them, either way
func TestExtensionVersionOrder(t *testing.T) {
	_, ctx := harness.Begin(t)
	defer harness.End(t)

	for order, want := range map[string][]string{
		"asc":  {"1.9", "1.10", "2147483648.0", "1.0beta"},
		"desc": {"2147483648.0", "1.10", "1.9", "1.0beta"},
	} {
		bind := pg.NewBind()
		_, err := (&schema.ExtensionListRequest{Sort: "default_version", Order: order}).Select(bind, pg.List)
		require.NoError(t, err)
		var got versions
		require.NoError(t, harness.Pool().With("orderby", bind.Get("orderby")).List(ctx, &got, versionQuery(`SELECT "default_version" FROM (VALUES ('a', '1.10'), ('b', '2147483648.0'), ('c', '1.0beta'), ('d', '1.9')) AS q("name", "default_version") ${orderby}`)))
		assert.Equal(t, want, []string(got), order)
	}
}

// versions are the rows of a query, of a version each
type versions []string

func (v *versions) Scan(row pg.Row) error {
	var version string
	if err := row.Scan(&version); err != nil {
		return err
	}
	*v = append(*v, version)
	return nil
}

// versionQuery is a query, as a list's selector
type versionQuery string

func (q versionQuery) Select(*pg.Bind, pg.Op) (string, error) {
	return string(q), nil
}
