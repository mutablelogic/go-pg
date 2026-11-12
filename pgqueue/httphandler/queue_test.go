package httphandler_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

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
			assert.Equal(t, name, result.Body[i].Name)
			require.NotNil(t, result.Body[i].Retries)
			assert.EqualValues(t, 3, *result.Body[i].Retries)
		}
	})

	t.Run("Page", func(t *testing.T) {
		result := list(t, "?offset=1&limit=1")
		assert.EqualValues(t, 3, result.Count)
		require.Len(t, result.Body, 1)
		assert.Equal(t, "three", result.Body[0].Name)
		assert.EqualValues(t, 1, result.Offset)
		require.NotNil(t, result.Limit)
		assert.EqualValues(t, 1, *result.Limit)
	})

	t.Run("BadQuery", func(t *testing.T) {
		status, body := server.Do(t, http.MethodGet, "queue?limit=many", nil)
		assert.Equal(t, http.StatusBadRequest, status, "body: %s", body)
	})
}

func TestGetQueue(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := test.IsolatedManager(t)
	server := test.NewServer(t, manager)
	_, err := manager.RegisterQueue(ctx, "queue", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)

	t.Run("Found", func(t *testing.T) {
		var queue schema.QueueWithStatus
		require.Equal(t, http.StatusOK, server.JSON(t, http.MethodGet, "queue/queue", nil, &queue))
		assert.Equal(t, "queue", queue.Name)
		assert.Zero(t, queue.New)
	})

	t.Run("NotFound", func(t *testing.T) {
		assert.Equal(t, http.StatusNotFound, server.JSON(t, http.MethodGet, "queue/missing", nil, nil))
	})

	t.Run("InvalidName", func(t *testing.T) {
		assert.Equal(t, http.StatusBadRequest, server.JSON(t, http.MethodGet, "queue/not%20a%20name", nil, nil))
	})
}

func TestUpdateQueue(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := test.IsolatedManager(t)
	server := test.NewServer(t, manager)
	_, err := manager.RegisterQueue(ctx, "queue", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)

	t.Run("Update", func(t *testing.T) {
		// The settings in the body are changed, and the others kept
		var queue schema.Queue
		require.Equal(t, http.StatusOK, server.JSON(t, http.MethodPatch, "queue/queue", map[string]any{"ttl": 2 * time.Hour, "concurrency": 3}, &queue))
		require.NotNil(t, queue.TTL)
		assert.Equal(t, 2*time.Hour, *queue.TTL)
		require.NotNil(t, queue.Concurrency)
		assert.EqualValues(t, 3, *queue.Concurrency)
		require.NotNil(t, queue.Retries)
		assert.EqualValues(t, 3, *queue.Retries)
	})

	t.Run("Invalid", func(t *testing.T) {
		assert.Equal(t, http.StatusBadRequest, server.JSON(t, http.MethodPatch, "queue/queue", map[string]any{}, nil))
		assert.Equal(t, http.StatusBadRequest, server.JSON(t, http.MethodPatch, "queue/not%20a%20name", map[string]any{"concurrency": 1}, nil))
		assert.Equal(t, http.StatusNotFound, server.JSON(t, http.MethodPatch, "queue/missing", map[string]any{"concurrency": 1}, nil))
	})
}

func TestDeleteQueue(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := test.IsolatedManager(t)
	server := test.NewServer(t, manager)
	_, err := manager.RegisterQueue(ctx, "queue", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)
	created, err := manager.CreateTask(ctx, "queue", schema.TaskMeta{Payload: json.RawMessage(`{}`)})
	require.NoError(t, err)

	// The queue is deleted, with its tasks
	var queue schema.Queue
	require.Equal(t, http.StatusOK, server.JSON(t, http.MethodDelete, "queue/queue", nil, &queue))
	assert.Equal(t, "queue", queue.Name)
	assert.Equal(t, http.StatusNotFound, server.JSON(t, http.MethodGet, "queue/queue", nil, nil))
	_, err = manager.GetTask(ctx, created.Id)
	assert.Error(t, err)

	assert.Equal(t, http.StatusNotFound, server.JSON(t, http.MethodDelete, "queue/queue", nil, nil))
	assert.Equal(t, http.StatusBadRequest, server.JSON(t, http.MethodDelete, "queue/not%20a%20name", nil, nil))
}

func TestCleanQueue(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := test.IsolatedManager(t)
	server := test.NewServer(t, manager)
	_, err := manager.RegisterQueue(ctx, "queue", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)
	done, err := manager.CreateTask(ctx, "queue", schema.TaskMeta{Payload: json.RawMessage(`{"n":1}`)})
	require.NoError(t, err)
	_, err = manager.CreateTask(ctx, "queue", schema.TaskMeta{Payload: json.RawMessage(`{"n":2}`)})
	require.NoError(t, err)

	// A task which finished longer ago than the retention period is deleted,
	// and the new task is kept
	require.NoError(t, manager.With("tid", done.Id).Exec(ctx, `UPDATE ${"schema"}."task" SET "started_at" = NOW() - INTERVAL '2 days', "finished_at" = NOW() - INTERVAL '2 days' WHERE "id" = @tid`))
	var cleaned schema.QueueCleanResponse
	require.Equal(t, http.StatusOK, server.JSON(t, http.MethodPost, "queue/queue/clean", nil, &cleaned))
	assert.Equal(t, "queue", cleaned.Queue)
	require.Len(t, cleaned.Body, 1)
	assert.Equal(t, done.Id, cleaned.Body[0].Id)

	var again schema.QueueCleanResponse
	require.Equal(t, http.StatusOK, server.JSON(t, http.MethodPost, "queue/queue/clean", nil, &again))
	assert.Empty(t, again.Body)
	assert.Equal(t, http.StatusBadRequest, server.JSON(t, http.MethodPost, "queue/not%20a%20name/clean", nil, nil))
}
