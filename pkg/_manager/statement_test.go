package manager_test

import (
	"context"
	"testing"

	// Packages
	manager "github.com/mutablelogic/go-pg/pkg/manager"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	assert "github.com/stretchr/testify/assert"
)

////////////////////////////////////////////////////////////////////////////////
// STAT STATEMENTS TESTS

func Test_Manager_StatStatementsAvailable(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	// Check that pg_stat_statements is available
	assert.True(mgr.StatStatementsAvailable(), "pg_stat_statements should be available")
}

func Test_Manager_ListStatements(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("ListAll", func(t *testing.T) {
		list, err := mgr.ListStatements(context.TODO(), schema.StatementListRequest{})
		assert.NoError(err)
		assert.NotNil(list)
		t.Logf("Found %d statements", list.Count)
	})

	t.Run("SortByCalls", func(t *testing.T) {
		list, err := mgr.ListStatements(context.TODO(), schema.StatementListRequest{
			Sort: "calls",
		})
		assert.NoError(err)
		assert.NotNil(list)
		t.Logf("Found %d statements sorted by calls", list.Count)
	})

	t.Run("SortByTotalTime", func(t *testing.T) {
		list, err := mgr.ListStatements(context.TODO(), schema.StatementListRequest{
			Sort: "total_ms",
		})
		assert.NoError(err)
		assert.NotNil(list)
	})
}

func Test_Manager_ResetStatements(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	// Reset statements
	err = mgr.ResetStatements(context.TODO())
	assert.NoError(err)

	// List again - should be empty or near empty
	list, err := mgr.ListStatements(context.TODO(), schema.StatementListRequest{})
	assert.NoError(err)
	assert.NotNil(list)
	t.Logf("After reset: %d statements", list.Count)
}
