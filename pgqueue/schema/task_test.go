package schema_test

import (
	"crypto/sha256"
	"encoding/json"
	"sync"
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

// taskHash is the hash of the task with the id
type taskHash struct {
	id   uint64
	hash []byte
}

func (h taskHash) Select(bind *pg.Bind, op pg.Op) (string, error) {
	bind.Set("id", h.id)
	return `SELECT "hash" FROM ${"schema"}."task" WHERE "id" = @id`, nil
}

func (h *taskHash) Scan(row pg.Row) error {
	return row.Scan(&h.hash)
}

func TestTaskHash(t *testing.T) {
	manager, ctx := test.Begin(t)
	defer test.End(t)

	// Two queues, with names for this run, as the shared schema is kept
	// between runs
	one, two := queueName("hash_one"), queueName("hash_two")
	for _, name := range []string{one, two} {
		var queue schema.Queue
		require.NoError(t, manager.Insert(ctx, &queue, schema.Queue{Queue: name}))
	}

	// hash inserts a task with the payload, and returns its hash
	hash := func(t *testing.T, queue, payload string) []byte {
		t.Helper()
		var id schema.TaskId
		require.NoError(t, manager.With("id", queue).Insert(ctx, &id, schema.TaskMeta{Payload: json.RawMessage(payload)}))
		result := taskHash{id: uint64(id)}
		require.NoError(t, manager.Get(ctx, &result, result))
		return result.hash
	}

	t.Run("Computed", func(t *testing.T) {
		// The hash is of the queue and the payload, as Postgres prints it
		want := sha256.Sum256([]byte(one + `:{"a": 1, "b": [true, null]}`))
		assert.Equal(t, want[:], hash(t, one, `{"a":1,"b":[true,null]}`))
	})

	t.Run("Same", func(t *testing.T) {
		// The same payload, with other spacing and key order, has the same hash
		assert.Equal(t, hash(t, one, `{"a":1,"b":2}`), hash(t, one, ` { "b" : 2, "a" : 1 } `))
	})

	t.Run("Nulls", func(t *testing.T) {
		// A null field is the same as a missing one, also in nested objects,
		// but a null in an array isn't left out
		assert.Equal(t, hash(t, one, `{"a":1}`), hash(t, one, `{"a":1,"b":null}`))
		assert.Equal(t, hash(t, one, `{"a":{"b":1}}`), hash(t, one, `{"a":{"b":1,"c":null}}`))
		assert.NotEqual(t, hash(t, one, `{"a":[1]}`), hash(t, one, `{"a":[1,null]}`))
	})

	t.Run("Different", func(t *testing.T) {
		// Another payload, or the same payload in another queue, has another
		// hash
		assert.NotEqual(t, hash(t, one, `{"a":1}`), hash(t, one, `{"a":2}`))
		assert.NotEqual(t, hash(t, one, `{"a":1}`), hash(t, two, `{"a":1}`))
	})

	t.Run("Upgrade", func(t *testing.T) {
		// A task table without the column, from before it was added, gets
		// the column, with the hash of each task, when the manager is created
		var id schema.TaskId
		require.NoError(t, manager.With("id", one).Insert(ctx, &id, schema.TaskMeta{Payload: json.RawMessage(`{"upgrade":true}`)}))
		require.NoError(t, manager.Exec(ctx, `ALTER TABLE ${"schema"}."task" DROP COLUMN "hash"`))
		_, err := pgqueue.New(ctx, manager.PoolConn, pgqueue.WithSchema(test.Schema()))
		require.NoError(t, err)

		want := sha256.Sum256([]byte(one + `:{"upgrade": true}`))
		result := taskHash{id: uint64(id)}
		require.NoError(t, manager.Get(ctx, &result, result))
		assert.Equal(t, want[:], result.hash)
	})
}

func TestTaskDedupe(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := test.IsolatedManager(t)

	// A queue which deduplicates tasks, and one which doesn't
	var queue schema.Queue
	require.NoError(t, manager.Insert(ctx, &queue, schema.Queue{Queue: "dedupe_on"}))
	require.NoError(t, manager.Update(ctx, &queue, schema.QueueName("dedupe_on"), schema.Queue{QueueMeta: schema.QueueMeta{Dedupe: types.Ptr(true)}}))
	require.NoError(t, manager.Insert(ctx, &queue, schema.Queue{Queue: "dedupe_off"}))

	// insert inserts a task, and returns its id
	insert := func(t *testing.T, queue, payload string, delayedAt *time.Time) uint64 {
		t.Helper()
		var id schema.TaskId
		require.NoError(t, manager.With("id", queue).Insert(ctx, &id, schema.TaskMeta{Payload: json.RawMessage(payload), DelayedAt: delayedAt}))
		return uint64(id)
	}

	// get returns the task with the id
	get := func(t *testing.T, id uint64) schema.TaskWithStatus {
		t.Helper()
		var task schema.TaskWithStatus
		require.NoError(t, manager.Get(ctx, &task, schema.TaskId(id)))
		return task
	}

	// set sets columns of the task with the id
	set := func(t *testing.T, id uint64, columns string) {
		t.Helper()
		require.NoError(t, manager.With("tid", id).Exec(ctx, `UPDATE ${"schema"}."task" SET `+columns+` WHERE "id" = @tid`))
	}

	t.Run("Same", func(t *testing.T) {
		// The same pending task is returned, also with other spacing
		id := insert(t, "dedupe_on", `{"same":1}`, nil)
		assert.Equal(t, id, insert(t, "dedupe_on", `{ "same" : 1 }`, nil))
	})

	t.Run("Different", func(t *testing.T) {
		assert.NotEqual(t, insert(t, "dedupe_on", `{"different":1}`, nil), insert(t, "dedupe_on", `{"different":2}`, nil))
	})

	t.Run("Off", func(t *testing.T) {
		// A queue which doesn't deduplicate inserts every task
		assert.NotEqual(t, insert(t, "dedupe_off", `{"off":1}`, nil), insert(t, "dedupe_off", `{"off":1}`, nil))
	})

	t.Run("Running", func(t *testing.T) {
		// A running task isn't the same, so another is inserted
		id := insert(t, "dedupe_on", `{"running":1}`, nil)
		set(t, id, `"started_at" = NOW(), "worker" = 'test'`)
		assert.NotEqual(t, id, insert(t, "dedupe_on", `{"running":1}`, nil))
	})

	t.Run("Finished", func(t *testing.T) {
		id := insert(t, "dedupe_on", `{"finished":1}`, nil)
		set(t, id, `"started_at" = NOW(), "finished_at" = NOW()`)
		assert.NotEqual(t, id, insert(t, "dedupe_on", `{"finished":1}`, nil))
	})

	t.Run("Failed", func(t *testing.T) {
		// A task which failed, with no retries left, isn't pending
		id := insert(t, "dedupe_on", `{"failed":1}`, nil)
		set(t, id, `"retries" = 0`)
		assert.Equal(t, "failed", get(t, id).Status)
		assert.NotEqual(t, id, insert(t, "dedupe_on", `{"failed":1}`, nil))
	})

	t.Run("Earlier", func(t *testing.T) {
		// A new task runs at the earlier of the two delays
		later := time.Now().Add(2 * time.Hour)
		sooner := time.Now().Add(time.Hour)
		id := insert(t, "dedupe_on", `{"earlier":1}`, &later)
		assert.Equal(t, id, insert(t, "dedupe_on", `{"earlier":1}`, &sooner))
		require.NotNil(t, get(t, id).DelayedAt)
		assert.WithinDuration(t, sooner, *get(t, id).DelayedAt, time.Second)

		// A later delay doesn't delay it more
		assert.Equal(t, id, insert(t, "dedupe_on", `{"earlier":1}`, &later))
		assert.WithinDuration(t, sooner, *get(t, id).DelayedAt, time.Second)

		// No delay runs it as soon as possible
		assert.Equal(t, id, insert(t, "dedupe_on", `{"earlier":1}`, nil))
		assert.Nil(t, get(t, id).DelayedAt)
	})

	t.Run("Retry", func(t *testing.T) {
		// A task waiting to be retried is the same, but keeps its backoff
		id := insert(t, "dedupe_on", `{"retry":1}`, nil)
		set(t, id, `"retries" = "retries" - 1, "delayed_at" = NOW() + INTERVAL '1 hour'`)
		assert.Equal(t, "retry", get(t, id).Status)
		assert.Equal(t, id, insert(t, "dedupe_on", `{"retry":1}`, nil))
		require.NotNil(t, get(t, id).DelayedAt)
		assert.WithinDuration(t, time.Now().Add(time.Hour), *get(t, id).DelayedAt, time.Minute)
	})

	t.Run("Concurrent", func(t *testing.T) {
		// Tasks inserted at the same time are the same task
		var wg sync.WaitGroup
		ids := make([]uint64, 10)
		for i := range ids {
			wg.Go(func() {
				var id schema.TaskId
				if err := manager.With("id", "dedupe_on").Insert(ctx, &id, schema.TaskMeta{Payload: json.RawMessage(`{"concurrent":1}`)}); err != nil {
					t.Error(err)
				}
				ids[i] = uint64(id)
			})
		}
		wg.Wait()
		for _, id := range ids {
			assert.Equal(t, ids[0], id)
		}
	})
}
