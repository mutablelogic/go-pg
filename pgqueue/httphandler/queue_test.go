package httphandler_test

import (
	"net/http"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	test "github.com/mutablelogic/go-pg/pgqueue/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestListQueues(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := test.IsolatedManager(t)
	server := test.NewServer(t, manager)

	// list returns the queues for the query
	list := func(t *testing.T, query string) schema.QueueList {
		t.Helper()
		var list schema.QueueList
		require.Equal(t, http.StatusOK, server.JSON(t, http.MethodGet, "queue"+query, nil, &list))
		return list
	}

	t.Run("Empty", func(t *testing.T) {
		result := list(t, "")
		assert.Zero(t, result.Count)
		assert.Empty(t, result.Body)
	})

	// Three queues
	for _, name := range []string{"two", "one", "three"} {
		_, err := manager.RegisterQueue(ctx, name, schema.QueueMeta{}, noopTask)
		require.NoError(t, err)
	}

	t.Run("List", func(t *testing.T) {
		// The queues are in name order, with their settings
		result := list(t, "")
		assert.EqualValues(t, 3, result.Count)
		require.Len(t, result.Body, 3)
		for i, name := range []string{"one", "three", "two"} {
			assert.Equal(t, name, result.Body[i].Queue)
			require.NotNil(t, result.Body[i].Retries)
			assert.EqualValues(t, 3, *result.Body[i].Retries)
		}
	})

	t.Run("Page", func(t *testing.T) {
		result := list(t, "?offset=1&limit=1")
		assert.EqualValues(t, 3, result.Count)
		require.Len(t, result.Body, 1)
		assert.Equal(t, "three", result.Body[0].Queue)
		assert.EqualValues(t, 1, result.Offset)
		require.NotNil(t, result.Limit)
		assert.EqualValues(t, 1, *result.Limit)
	})

	t.Run("BadQuery", func(t *testing.T) {
		status, body := server.Do(t, http.MethodGet, "queue?limit=many", nil)
		assert.Equal(t, http.StatusBadRequest, status, "body: %s", body)
	})
}
