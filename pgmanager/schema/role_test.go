package schema_test

import (
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
)

func Test_RoleListRequest_Predefined(t *testing.T) {
	// The roles are the others, unless predefined is true, by name
	for _, test := range []struct {
		predefined *bool
		where      string
	}{
		{nil, `WHERE NOT "predefined"`},
		{types.Ptr(false), `WHERE NOT "predefined"`},
		{types.Ptr(true), `WHERE "predefined"`},
	} {
		bind := pg.NewBind()
		_, err := (&schema.RoleListRequest{Predefined: test.predefined}).Select(bind, pg.List)
		assert.NoError(t, err)
		assert.Equal(t, test.where, bind.Get("where"))
		assert.Equal(t, `ORDER BY "rolname"`, bind.Get("orderby"))
	}

	// It's in the query, when it's set
	assert.Equal(t, "predefined=true", schema.RoleListRequest{Predefined: types.Ptr(true)}.Query().Encode())
	assert.Equal(t, "", schema.RoleListRequest{}.Query().Encode())
}
