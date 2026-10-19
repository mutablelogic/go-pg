package httphandler_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	test "github.com/mutablelogic/go-pg/pgqueue/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestListTasks(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := test.IsolatedManager(t)
	server := test.NewServer(t, manager)

	// list returns the tasks for the query
	list := func(t *testing.T, query string) schema.TaskList {
		t.Helper()
		var list schema.TaskList
		require.Equal(t, http.StatusOK, server.JSON(t, http.MethodGet, "task"+query, nil, &list))
		return list
	}

	t.Run("Empty", func(t *testing.T) {
		result := list(t, "")
		assert.Zero(t, result.Count)
		assert.Empty(t, result.Body)
	})

	// Three tasks, one of which is running, and a task in each of two other
	// queues
	for _, queue := range []string{"queue", "other", "another"} {
		_, err := manager.RegisterQueue(ctx, queue, schema.QueueMeta{}, noopTask)
		require.NoError(t, err)
	}
	for i := range 3 {
		_, err := manager.CreateTask(ctx, "queue", schema.TaskMeta{Payload: json.RawMessage(`{"n":` + strconv.Itoa(i) + `}`)})
		require.NoError(t, err)
	}
	running, err := manager.NextTask(ctx, "worker", "queue")
	require.NoError(t, err)
	require.NotNil(t, running)
	for _, queue := range []string{"other", "another"} {
		_, err := manager.CreateTask(ctx, queue, schema.TaskMeta{Payload: json.RawMessage(`{}`)})
		require.NoError(t, err)
	}

	t.Run("All", func(t *testing.T) {
		// The tasks in every queue, with any status
		assert.EqualValues(t, 5, list(t, "").Count)
	})

	t.Run("Queues", func(t *testing.T) {
		// The tasks in one queue, or in more than one
		assert.EqualValues(t, 1, list(t, "?queue=other").Count)
		result := list(t, "?queue=other&queue=another")
		assert.EqualValues(t, 2, result.Count)
		for _, task := range result.Body {
			assert.Contains(t, []string{"other", "another"}, task.Queue)
		}
	})

	t.Run("List", func(t *testing.T) {
		// The tasks are oldest first, with their queue, payload and status
		result := list(t, "?queue=queue")
		assert.EqualValues(t, 3, result.Count)
		require.Len(t, result.Body, 3)
		for i, task := range result.Body {
			assert.Equal(t, "queue", task.Queue)
			assert.JSONEq(t, `{"n":`+strconv.Itoa(i)+`}`, string(task.Payload))
		}
		assert.Equal(t, running.Id, result.Body[0].Id)
		assert.Equal(t, "running", result.Body[0].Status)
		assert.Equal(t, "new", result.Body[1].Status)
	})

	t.Run("Status", func(t *testing.T) {
		result := list(t, "?status=running")
		assert.EqualValues(t, 1, result.Count)
		require.Len(t, result.Body, 1)
		assert.Equal(t, running.Id, result.Body[0].Id)
		require.NotNil(t, result.Body[0].Worker)
		assert.Equal(t, "worker", *result.Body[0].Worker)

		result = list(t, "?queue=queue&status=new")
		assert.EqualValues(t, 2, result.Count)
	})

	t.Run("Page", func(t *testing.T) {
		result := list(t, "?offset=4&limit=10")
		assert.EqualValues(t, 5, result.Count)
		assert.Len(t, result.Body, 1)
	})

	t.Run("BadQuery", func(t *testing.T) {
		for _, query := range []string{"?status=unknown", "?status=any", "?limit=many", "?queue=not+a+queue"} {
			status, body := server.Do(t, http.MethodGet, "task"+query, nil)
			assert.Equal(t, http.StatusBadRequest, status, "%s: %s", query, body)
		}
	})
}

func TestGetTask(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := test.IsolatedManager(t)
	server := test.NewServer(t, manager)
	_, err := manager.RegisterQueue(ctx, "queue", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)
	created, err := manager.CreateTask(ctx, "queue", schema.TaskMeta{Payload: json.RawMessage(`{}`)})
	require.NoError(t, err)

	t.Run("Found", func(t *testing.T) {
		var task schema.TaskWithStatus
		require.Equal(t, http.StatusOK, server.JSON(t, http.MethodGet, "task/"+strconv.FormatUint(created.Id, 10), nil, &task))
		assert.Equal(t, created.Id, task.Id)
		assert.Equal(t, "new", task.Status)
	})

	t.Run("NotFound", func(t *testing.T) {
		assert.Equal(t, http.StatusNotFound, server.JSON(t, http.MethodGet, "task/"+strconv.FormatUint(created.Id+1000, 10), nil, nil))
	})

	t.Run("InvalidId", func(t *testing.T) {
		assert.Equal(t, http.StatusBadRequest, server.JSON(t, http.MethodGet, "task/abc", nil, nil))

		// An id is a signed BIGINT, so the largest is 2^63-1, which isn't
		// found, and one more is invalid
		assert.Equal(t, http.StatusNotFound, server.JSON(t, http.MethodGet, "task/9223372036854775807", nil, nil))
		assert.Equal(t, http.StatusBadRequest, server.JSON(t, http.MethodGet, "task/9223372036854775808", nil, nil))
	})
}
