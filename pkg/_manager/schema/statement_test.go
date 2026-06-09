package schema_test

import (
	"encoding/json"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_Statement_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidStatement", func(t *testing.T) {
		s := schema.Statement{
			Role:     "testuser",
			Database: "testdb",
			QueryID:  12345,
			Query:    "SELECT * FROM users",
			Calls:    100,
			Rows:     500,
			Total:    1234.56,
			Min:      0.5,
			Max:      50.0,
			Mean:     12.34,
		}
		str := s.String()
		assert.NotEmpty(str)

		// Verify it's valid JSON
		var parsed schema.Statement
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal(s.QueryID, parsed.QueryID)
		assert.Equal(s.Query, parsed.Query)
	})

	t.Run("EmptyStatement", func(t *testing.T) {
		s := schema.Statement{}
		str := s.String()
		assert.NotEmpty(str)

		// Verify it's valid JSON
		var parsed schema.Statement
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
	})
}

func Test_StatementList_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("WithStatements", func(t *testing.T) {
		l := schema.StatementList{
			Count: 2,
			Body: []schema.Statement{
				{QueryID: 1, Query: "SELECT 1"},
				{QueryID: 2, Query: "SELECT 2"},
			},
		}
		str := l.String()
		assert.NotEmpty(str)

		// Verify it's valid JSON
		var parsed schema.StatementList
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
		assert.Equal(uint64(2), parsed.Count)
		assert.Len(parsed.Body, 2)
	})

	t.Run("EmptyList", func(t *testing.T) {
		l := schema.StatementList{}
		str := l.String()
		assert.NotEmpty(str)

		// Verify it's valid JSON
		var parsed schema.StatementList
		err := json.Unmarshal([]byte(str), &parsed)
		assert.NoError(err)
	})
}

func Test_StatementListRequest_Select(t *testing.T) {
	assert := assert.New(t)

	t.Run("ListOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.StatementListRequest{}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("ORDER BY database ASC, queryid ASC", bind.Get("orderby"))
		assert.Equal("", bind.Get("where"))
	})

	t.Run("UnsupportedOperation", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.StatementListRequest{}
		_, err := req.Select(bind, pg.Get)
		assert.Error(err)
	})

	t.Run("FilterByDatabase", func(t *testing.T) {
		bind := pg.NewBind()
		db := "testdb"
		req := schema.StatementListRequest{Database: &db}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("testdb", bind.Get("database"))
		assert.Contains(bind.Get("where"), "d.datname = @database")
	})

	t.Run("FilterByRole", func(t *testing.T) {
		bind := pg.NewBind()
		role := "testuser"
		req := schema.StatementListRequest{Role: &role}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		assert.Equal("testuser", bind.Get("role"))
		assert.Contains(bind.Get("where"), "u.rolname = @role")
	})

	t.Run("FilterByDatabaseAndRole", func(t *testing.T) {
		bind := pg.NewBind()
		db := "testdb"
		role := "testuser"
		req := schema.StatementListRequest{Database: &db, Role: &role}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
		where := bind.Get("where").(string)
		assert.Contains(where, "d.datname = @database")
		assert.Contains(where, "u.rolname = @role")
		assert.Contains(where, " AND ")
	})

	t.Run("EmptyDatabaseFilter", func(t *testing.T) {
		bind := pg.NewBind()
		db := ""
		req := schema.StatementListRequest{Database: &db}
		_, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.Equal("", bind.Get("where"))
	})

	t.Run("EmptyRoleFilter", func(t *testing.T) {
		bind := pg.NewBind()
		role := ""
		req := schema.StatementListRequest{Role: &role}
		_, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.Equal("", bind.Get("where"))
	})
}

func Test_StatementListRequest_Sort(t *testing.T) {
	assert := assert.New(t)

	t.Run("SortByCalls", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.StatementListRequest{Sort: "calls"}
		_, err := req.Select(bind, pg.List)
		assert.NoError(err)
		orderby := bind.Get("orderby").(string)
		assert.Contains(orderby, "calls DESC")
	})

	t.Run("SortByRows", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.StatementListRequest{Sort: "rows"}
		_, err := req.Select(bind, pg.List)
		assert.NoError(err)
		orderby := bind.Get("orderby").(string)
		assert.Contains(orderby, "rows DESC")
	})

	t.Run("SortByTotalMs", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.StatementListRequest{Sort: "total_ms"}
		_, err := req.Select(bind, pg.List)
		assert.NoError(err)
		orderby := bind.Get("orderby").(string)
		assert.Contains(orderby, "total_exec_time DESC")
	})

	t.Run("SortByMinMs", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.StatementListRequest{Sort: "min_ms"}
		_, err := req.Select(bind, pg.List)
		assert.NoError(err)
		orderby := bind.Get("orderby").(string)
		assert.Contains(orderby, "min_exec_time ASC")
	})

	t.Run("SortByMaxMs", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.StatementListRequest{Sort: "max_ms"}
		_, err := req.Select(bind, pg.List)
		assert.NoError(err)
		orderby := bind.Get("orderby").(string)
		assert.Contains(orderby, "max_exec_time DESC")
	})

	t.Run("SortByMeanMs", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.StatementListRequest{Sort: "mean_ms"}
		_, err := req.Select(bind, pg.List)
		assert.NoError(err)
		orderby := bind.Get("orderby").(string)
		assert.Contains(orderby, "mean_exec_time DESC")
	})

	t.Run("SortCaseInsensitive", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.StatementListRequest{Sort: "CALLS"}
		_, err := req.Select(bind, pg.List)
		assert.NoError(err)
		orderby := bind.Get("orderby").(string)
		assert.Contains(orderby, "calls DESC")
	})

	t.Run("InvalidSort", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.StatementListRequest{Sort: "invalid"}
		_, err := req.Select(bind, pg.List)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("AlwaysOrderByDatabaseAndQueryId", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.StatementListRequest{Sort: "calls"}
		_, err := req.Select(bind, pg.List)
		assert.NoError(err)
		orderby := bind.Get("orderby").(string)
		assert.Contains(orderby, "ORDER BY database ASC, queryid ASC")
	})
}

func Test_StatementListRequest_OffsetLimit(t *testing.T) {
	assert := assert.New(t)

	t.Run("DefaultLimit", func(t *testing.T) {
		bind := pg.NewBind()
		req := schema.StatementListRequest{}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
	})

	t.Run("CustomOffsetLimit", func(t *testing.T) {
		bind := pg.NewBind()
		limit := uint64(50)
		req := schema.StatementListRequest{
			OffsetLimit: pg.OffsetLimit{Offset: 10, Limit: &limit},
		}
		sql, err := req.Select(bind, pg.List)
		assert.NoError(err)
		assert.NotEmpty(sql)
	})
}
