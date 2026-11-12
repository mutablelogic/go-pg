package schema_test

import (
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_ConnectionListRequest_Sort(t *testing.T) {
	tests := []struct {
		sort, order, orderby string
	}{
		{"", "", `ORDER BY "pid" ASC NULLS LAST`},
		{"database", "", `ORDER BY "database" ASC NULLS LAST, "pid" ASC`},
		{"client", "asc", `ORDER BY NULLIF("client_addr", '') ASC NULLS LAST, "pid" ASC`},
		{"client", "desc", `ORDER BY NULLIF("client_addr", '') DESC NULLS LAST, "pid" ASC`},
		{"Query_Start", "ASC", `ORDER BY "query_start" ASC NULLS LAST, "pid" ASC`},
	}
	for _, test := range tests {
		t.Run(test.sort+"_"+test.order, func(t *testing.T) {
			bind := pg.NewBind()
			req := schema.ConnectionListRequest{Sort: test.sort, Order: test.order}
			_, err := req.Select(bind, pg.List)
			assert.NoError(t, err)
			assert.Equal(t, test.orderby, bind.Get("orderby"))
		})
	}

	// Neither the query, nor an order which isn't asc or desc, are allowed
	_, err := (&schema.ConnectionListRequest{Sort: "query"}).Select(pg.NewBind(), pg.List)
	assert.ErrorIs(t, err, pg.ErrBadParameter)
	_, err = (&schema.ConnectionListRequest{Sort: "state", Order: "up"}).Select(pg.NewBind(), pg.List)
	assert.ErrorIs(t, err, pg.ErrBadParameter)
}
