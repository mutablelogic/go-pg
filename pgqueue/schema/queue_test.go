package schema_test

import (
	"crypto/rand"
	"strings"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	pgqueue "github.com/mutablelogic/go-pg/pgqueue"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	test "github.com/mutablelogic/go-pg/pgqueue/test"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	test.Main(m, nil)
}

func TestQueueDedupe(t *testing.T) {
	manager, ctx := test.Begin(t)
	defer test.End(t)

	// The queues have names for this run, as the shared schema is kept
	// between runs
	defaultQueue, updateQueue := queueName("dedupe_default"), queueName("dedupe_update")

	t.Run("Default", func(t *testing.T) {
		// A new queue doesn't deduplicate tasks
		var queue schema.Queue
		require.NoError(t, manager.Insert(ctx, &queue, schema.Queue{Name: defaultQueue}))
		assert.Equal(t, defaultQueue, queue.Name)
		require.NotNil(t, queue.Dedupe)
		assert.False(t, *queue.Dedupe)
	})

	t.Run("Update", func(t *testing.T) {
		var queue schema.Queue
		require.NoError(t, manager.Insert(ctx, &queue, schema.Queue{Name: updateQueue}))

		// Deduplication can be turned on, and is returned when the queue is got
		require.NoError(t, manager.Update(ctx, &queue, schema.QueueName(updateQueue), schema.Queue{QueueMeta: schema.QueueMeta{Dedupe: types.Ptr(true)}}))
		require.NotNil(t, queue.Dedupe)
		assert.True(t, *queue.Dedupe)

		var got schema.Queue
		require.NoError(t, manager.Get(ctx, &got, schema.QueueName(updateQueue)))
		require.NotNil(t, got.Dedupe)
		assert.True(t, *got.Dedupe)
	})

	t.Run("Upgrade", func(t *testing.T) {
		// A queue table without the column, from before it was added, gets
		// the column when the manager is created
		require.NoError(t, manager.Exec(ctx, `ALTER TABLE `+types.DoubleQuote(test.Schema())+`."queue" DROP COLUMN "dedupe"`))
		_, err := pgqueue.New(ctx, manager.PoolConn, pgqueue.WithSchema(test.Schema()))
		require.NoError(t, err)

		var queue schema.Queue
		require.NoError(t, manager.Get(ctx, &queue, schema.QueueName(defaultQueue)))
		require.NotNil(t, queue.Dedupe)
		assert.False(t, *queue.Dedupe)
	})

	t.Run("NoPatch", func(t *testing.T) {
		var queue schema.Queue
		err := manager.Update(ctx, &queue, schema.QueueName(defaultQueue), schema.Queue{})
		assert.Error(t, err)
		assert.NotErrorIs(t, err, pg.ErrNotFound)
	})
}

// queueName returns a queue name with the prefix, which is new for each run
func queueName(prefix string) string {
	return prefix + "_" + strings.ToLower(rand.Text())
}

func TestQueueTable(t *testing.T) {
	queue := schema.Queue{Name: "queue", QueueMeta: schema.QueueMeta{
		TTL:         types.Ptr(time.Hour),
		Retries:     types.Ptr(uint64(3)),
		RetryDelay:  types.Ptr(2 * time.Minute),
		Concurrency: types.Ptr(uint64(0)),
		Dedupe:      types.Ptr(true),
	}}
	cells := make([]any, len(queue.Header()))
	for i := range cells {
		cells[i] = queue.Cell(i)
	}
	assert.Equal(t, []any{"queue", "1h0m0s", "3", "2m0s", "unlimited", "yes"}, cells)

	// Settings which aren't set are empty, and a concurrency limit is shown
	queue = schema.Queue{Name: "limited", QueueMeta: schema.QueueMeta{Concurrency: types.Ptr(uint64(2))}}
	assert.Equal(t, "", queue.Cell(1))
	assert.Equal(t, "2", queue.Cell(4))
	assert.Equal(t, "", queue.Cell(5))
}
