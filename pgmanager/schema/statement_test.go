package schema_test

import (
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_StatementListRequest_Sort(t *testing.T) {
	tests := []struct {
		sort, order, orderby string
	}{
		{"", "", "ORDER BY queryid ASC"},
		{"calls", "", "ORDER BY calls DESC"},
		{"calls", "asc", "ORDER BY calls ASC"},
		{"min_ms", "", "ORDER BY min_exec_time ASC"},
		{"min_ms", "desc", "ORDER BY min_exec_time DESC"},
		{"total_ms", "DESC", "ORDER BY total_exec_time DESC"},
	}
	for _, test := range tests {
		t.Run(test.sort+"_"+test.order, func(t *testing.T) {
			bind := pg.NewBind()
			req := schema.StatementListRequest{Sort: test.sort, Order: test.order}
			_, err := req.Select(bind, pg.List)
			assert.NoError(t, err)
			assert.Equal(t, test.orderby, bind.Get("orderby"))
		})
	}

	// An order which isn't asc or desc is a bad parameter
	req := schema.StatementListRequest{Sort: "calls", Order: "up"}
	_, err := req.Select(pg.NewBind(), pg.List)
	assert.ErrorIs(t, err, pg.ErrBadParameter)
}
