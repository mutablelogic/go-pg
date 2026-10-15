package httpclient_test

import (
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestListQueues(t *testing.T) {
	manager, client, ctx := newClient(t)

	t.Run("Empty", func(t *testing.T) {
		list, err := client.ListQueues(ctx, schema.QueueListRequest{})
		require.NoError(t, err)
		assert.Zero(t, list.Count)
		assert.Empty(t, list.Body)
	})

	// Three queues, one with its own settings
	for _, name := range []string{"two", "one"} {
		_, err := manager.RegisterQueue(ctx, name, schema.QueueMeta{}, noopTask)
		require.NoError(t, err)
	}
	_, err := manager.RegisterQueue(ctx, "three", schema.QueueMeta{
		TTL:        types.Ptr(3 * time.Hour),
		RetryDelay: types.Ptr(5 * time.Minute),
		Dedupe:     types.Ptr(true),
	}, noopTask)
	require.NoError(t, err)

	t.Run("List", func(t *testing.T) {
		// The queues are in name order, with their settings, including their
		// durations
		list, err := client.ListQueues(ctx, schema.QueueListRequest{})
		require.NoError(t, err)
		assert.EqualValues(t, 3, list.Count)
		require.Len(t, list.Body, 3)
		names := make([]string, len(list.Body))
		for i, queue := range list.Body {
			names[i] = queue.Queue
		}
		assert.Equal(t, []string{"one", "three", "two"}, names)

		three := list.Body[1]
		require.NotNil(t, three.TTL)
		assert.Equal(t, 3*time.Hour, *three.TTL)
		require.NotNil(t, three.RetryDelay)
		assert.Equal(t, 5*time.Minute, *three.RetryDelay)
		require.NotNil(t, three.Dedupe)
		assert.True(t, *three.Dedupe)
	})

	t.Run("Page", func(t *testing.T) {
		list, err := client.ListQueues(ctx, schema.QueueListRequest{OffsetLimit: pg.OffsetLimit{Offset: 1, Limit: types.Ptr(uint64(1))}})
		require.NoError(t, err)
		assert.EqualValues(t, 3, list.Count)
		require.Len(t, list.Body, 1)
		assert.Equal(t, "three", list.Body[0].Queue)
		assert.EqualValues(t, 1, list.Offset)
		require.NotNil(t, list.Limit)
		assert.EqualValues(t, 1, *list.Limit)
	})
}
