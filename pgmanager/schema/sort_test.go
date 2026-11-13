package schema_test

import (
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_ListRequest_Sort(t *testing.T) {
	tests := []struct {
		name    string
		req     pg.Selector
		orderby string
	}{
		{"role unsorted", &schema.RoleListRequest{Order: "desc"}, `ORDER BY "rolname"`},
		{"role name", &schema.RoleListRequest{Sort: "name", Order: "desc"}, `ORDER BY "rolname" DESC NULLS LAST`},
		{"role conlimit", &schema.RoleListRequest{Sort: "ConLimit"}, `ORDER BY NULLIF("rolconnlimit", -1) ASC NULLS LAST, "rolname" ASC`},
		{"setting unsorted", &schema.SettingListRequest{}, `ORDER BY "category", "name"`},
		{"setting changed", &schema.SettingListRequest{Sort: "changed", Order: "DESC"}, `ORDER BY "changed" DESC NULLS LAST, "name" ASC`},
		{"tablespace location", &schema.TablespaceListRequest{Sort: "location"}, `ORDER BY NULLIF("location", '') ASC NULLS LAST, "name" ASC`},
		{"slot lag", &schema.ReplicationSlotListRequest{Sort: "lag", Order: "desc"}, `ORDER BY "lag_bytes" DESC NULLS LAST, "name" ASC`},
		{"extension version", &schema.ExtensionListRequest{Sort: "version"}, `ORDER BY CASE WHEN COALESCE("installed_version", "default_version") ~ '^[0-9]+(\.[0-9]+)*\Z' THEN string_to_array(COALESCE("installed_version", "default_version"), '.')::int[] END ASC NULLS LAST, COALESCE("installed_version", "default_version") ASC NULLS LAST, "name" ASC`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bind := pg.NewBind()
			_, err := test.req.Select(bind, pg.List)
			assert.NoError(t, err)
			assert.Equal(t, test.orderby, bind.Get("orderby"))
		})
	}

	// A sort which isn't one, or an order which isn't asc or desc, isn't
	// allowed
	for _, req := range []pg.Selector{
		&schema.RoleListRequest{Sort: "memberof"},
		&schema.SettingListRequest{Sort: "value"},
		&schema.TablespaceListRequest{Sort: "size", Order: "up"},
		&schema.ReplicationSlotListRequest{Sort: "client_addr"},
		&schema.ExtensionListRequest{Sort: "comment"},
	} {
		_, err := req.Select(pg.NewBind(), pg.List)
		assert.ErrorIs(t, err, pg.ErrBadParameter)
	}
}
