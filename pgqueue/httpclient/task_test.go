package httpclient_test

import (
	"encoding/json"
	"strconv"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestListTasks(t *testing.T) {
	manager, client, ctx := newClient(t)

	// Three tasks, one of which is running
	_, err := manager.RegisterQueue(ctx, "queue", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)
	for i := range 3 {
		_, err := manager.CreateTask(ctx, "queue", schema.TaskMeta{Payload: json.RawMessage(`{"n":` + strconv.Itoa(i) + `}`)})
		require.NoError(t, err)
	}
	running, err := manager.NextTask(ctx, "worker")
	require.NoError(t, err)
	require.NotNil(t, running)

	t.Run("List", func(t *testing.T) {
		list, err := client.ListTasks(ctx, schema.TaskListRequest{})
		require.NoError(t, err)
		assert.EqualValues(t, 3, list.Count)
		require.Len(t, list.Body, 3)
		assert.Equal(t, running.Id, list.Body[0].Id)
		assert.Equal(t, "running", list.Body[0].Status)
	})

	t.Run("Status", func(t *testing.T) {
		list, err := client.ListTasks(ctx, schema.TaskListRequest{Status: "new"})
		require.NoError(t, err)
		assert.EqualValues(t, 2, list.Count)
		for _, task := range list.Body {
			assert.Equal(t, "new", task.Status)
		}
	})

	t.Run("Queues", func(t *testing.T) {
		_, err := manager.RegisterQueue(ctx, "other", schema.QueueMeta{}, noopTask)
		require.NoError(t, err)
		_, err = manager.CreateTask(ctx, "other", schema.TaskMeta{Payload: json.RawMessage(`{}`)})
		require.NoError(t, err)

		list, err := client.ListTasks(ctx, schema.TaskListRequest{Queue: []string{"other"}})
		require.NoError(t, err)
		assert.EqualValues(t, 1, list.Count)
		list, err = client.ListTasks(ctx, schema.TaskListRequest{Queue: []string{"queue", "other"}})
		require.NoError(t, err)
		assert.EqualValues(t, 4, list.Count)
	})

	t.Run("Page", func(t *testing.T) {
		list, err := client.ListTasks(ctx, schema.TaskListRequest{Queue: []string{"queue"}, OffsetLimit: pg.OffsetLimit{Offset: 2, Limit: types.Ptr(uint64(10))}})
		require.NoError(t, err)
		assert.EqualValues(t, 3, list.Count)
		assert.Len(t, list.Body, 1)
	})
}

func TestGetTask(t *testing.T) {
	manager, client, ctx := newClient(t)
	_, err := manager.RegisterQueue(ctx, "queue", schema.QueueMeta{}, noopTask)
	require.NoError(t, err)
	created, err := manager.CreateTask(ctx, "queue", schema.TaskMeta{Payload: json.RawMessage(`{}`)})
	require.NoError(t, err)

	task, err := client.GetTask(ctx, created.Id)
	require.NoError(t, err)
	assert.Equal(t, created.Id, task.Id)
	assert.Equal(t, "new", task.Status)

	_, err = client.GetTask(ctx, created.Id+1000)
	assert.Error(t, err)
}
