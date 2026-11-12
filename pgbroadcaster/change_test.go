package broadcaster_test

import (
	"context"
	"strings"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	broadcaster "github.com/mutablelogic/go-pg/pgbroadcaster"
	schema "github.com/mutablelogic/go-pg/pgbroadcaster/schema"
	test "github.com/mutablelogic/go-pg/pgbroadcaster/test"
	changes "github.com/mutablelogic/go-server/broadcaster"
	changeschema "github.com/mutablelogic/go-server/broadcaster/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// watch watches the manager's changes which match the filter, with a
// channel of the size, until the returned function is called
func watch(t *testing.T, ctx context.Context, manager *broadcaster.Manager, filter changeschema.ChangeFilter, size int) (chan changeschema.Change, func()) {
	t.Helper()
	ch := make(chan changeschema.Change, size)
	watchCtx, cancel := context.WithCancel(ctx)
	done, err := manager.Watch(watchCtx, filter, ch)
	require.NoError(t, err)
	return ch, func() {
		cancel()
		assert.NoError(t, <-done)
	}
}

// receive returns the next change of the channel, or false when there's
// none within a few seconds
func receive(ch chan changeschema.Change) (changeschema.Change, bool) {
	select {
	case change := <-ch:
		return change, true
	case <-time.After(5 * time.Second):
		return changeschema.Change{}, false
	}
}

func TestPublish(t *testing.T) {
	// A published change is logged by the manager listening on its channel,
	// and only one run of a manager listens at a time
	manager, ctx := test.Begin(t)
	defer test.End(t)
	assert.True(t, manager.Running())
	assert.False(t, manager.Shared())
	assert.ErrorIs(t, manager.Run(ctx, nil), pg.ErrConflict)

	require.NoError(t, manager.Publish(ctx, changeschema.Change{Action: changeschema.ChangeUpdate, Schema: "filer", Table: "volume"}))
	require.Eventually(t, func() bool {
		return strings.Contains(test.Logs(), `payload="{\"action\":\"update\",\"schema\":\"filer\",\"table\":\"volume\"}"`)
	}, 10*time.Second, 10*time.Millisecond, test.Logs())

	// A change which is too large isn't published
	err := manager.Publish(ctx, changeschema.Change{Action: changeschema.ChangeUpdate, Table: strings.Repeat("x", schema.MaxPayloadSize)})
	assert.ErrorIs(t, err, pg.ErrBadParameter)
}

func TestWatch(t *testing.T) {
	manager, ctx := test.Begin(t)
	defer test.End(t)

	// Every watcher is sent a change which is published, and only the ones
	// which match its filter
	first, unwatchFirst := watch(t, ctx, manager, changeschema.ChangeFilter{}, 1)
	second, unwatchSecond := watch(t, ctx, manager, changeschema.ChangeFilter{}, 1)
	defer unwatchSecond()
	volumes, unwatchVolumes := watch(t, ctx, manager, changeschema.ChangeFilter{Schema: "filer", Table: "volume"}, 1)
	defer unwatchVolumes()
	created := changeschema.Change{Action: changeschema.ChangeCreate, Schema: "filer", Table: "object"}
	require.NoError(t, manager.Publish(ctx, created))
	for _, ch := range []chan changeschema.Change{first, second} {
		got, ok := receive(ch)
		require.True(t, ok, "the change wasn't sent")
		assert.Equal(t, created, got)
	}
	updated := changeschema.Change{Action: changeschema.ChangeUpdate, Schema: "filer", Table: "volume", Row: "42"}
	require.NoError(t, manager.Publish(ctx, updated))
	got, ok := receive(volumes)
	require.True(t, ok, "the change which matches wasn't sent")
	assert.Equal(t, updated, got)
	for _, ch := range []chan changeschema.Change{first, second} {
		_, ok := receive(ch)
		require.True(t, ok)
	}

	// A watcher which has stopped isn't sent changes, and one which isn't
	// ready for one misses it, which is logged
	unwatchFirst()
	full, unwatchFull := watch(t, ctx, manager, changeschema.ChangeFilter{}, 0)
	defer unwatchFull()
	require.NoError(t, manager.Publish(ctx, changeschema.Change{Action: changeschema.ChangeDelete, Table: "volume"}))
	got, ok = receive(second)
	require.True(t, ok)
	assert.Equal(t, "volume", got.Table)
	select {
	case <-first:
		t.Error("a channel which stopped watching was sent a change")
	case <-full:
		t.Error("a channel which wasn't ready was sent a change")
	case change := <-volumes:
		t.Errorf("a change which doesn't match the filter was sent: %v", change)
	default:
	}
	require.Eventually(t, func() bool { return strings.Contains(test.Logs(), "watchers missed a change") }, 5*time.Second, 10*time.Millisecond)
}

func TestWatchTriggers(t *testing.T) {
	// A notification of a trigger, whose action is its TG_OP, is a change,
	// with its row and data, and one which isn't a change, or whose action
	// isn't INSERT, UPDATE or DELETE, is logged, and skipped
	manager, ctx := test.Begin(t)
	defer test.End(t)
	ch, unwatch := watch(t, ctx, manager, changeschema.ChangeFilter{Schema: "triggers"}, 4)
	defer unwatch()
	skipped := strings.Count(test.Logs(), "not a change")
	notify := func(payload string) {
		t.Helper()
		require.NoError(t, test.Conn().With("channel", manager.Channel(), "payload", payload).Exec(ctx, `SELECT pg_notify(@channel, @payload)`))
	}
	notify(`not json`)
	notify(`{"schema":"triggers","table":"item","action":"TRUNCATE"}`)
	notify(`{"schema":"triggers","table":"item","action":"INSERT","row":"{\"id\":\"1\"}"}`)
	notify(`{"schema":"triggers","table":"item","action":"UPDATE","row":"2","data":{"serial":3}}`)
	notify(`{"schema":"triggers","table":"item","action":"DELETE"}`)
	for _, want := range []changeschema.Change{
		{Action: changeschema.ChangeCreate, Schema: "triggers", Table: "item", Row: `{"id":"1"}`},
		{Action: changeschema.ChangeUpdate, Schema: "triggers", Table: "item", Row: "2", Data: []byte(`{"serial":3}`)},
		{Action: changeschema.ChangeDelete, Schema: "triggers", Table: "item"},
	} {
		got, ok := receive(ch)
		require.True(t, ok, "%v wasn't sent", want)
		assert.Equal(t, want, got)
	}
	assert.Equal(t, skipped+2, strings.Count(test.Logs(), "not a change"), test.Logs())
}

func TestWatchStopped(t *testing.T) {
	// A manager of its own, which is stopped
	manager, stop, err := test.Start(test.Conn())
	require.NoError(t, err)
	require.NoError(t, manager.WaitListening(context.Background()))

	// A watch ends when the manager stops, as its broadcaster stops, and
	// none is made once it has stopped
	done, err := manager.Watch(context.Background(), changeschema.ChangeFilter{}, make(chan changeschema.Change, 1))
	require.NoError(t, err)
	require.NoError(t, stop())
	select {
	case err := <-done:
		assert.ErrorIs(t, err, httpresponse.ErrServiceUnavailable)
	case <-time.After(5 * time.Second):
		t.Fatal("the watch didn't end when the manager stopped")
	}
	_, err = manager.Watch(context.Background(), changeschema.ChangeFilter{}, make(chan changeschema.Change, 1))
	assert.ErrorIs(t, err, httpresponse.ErrServiceUnavailable)

	// A manager which isn't listening isn't waited for, once the context
	// is cancelled
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, manager.WaitListening(ctx), context.DeadlineExceeded)
}

func TestShared(t *testing.T) {
	// A manager with a shared broadcaster publishes the changes to it, and
	// doesn't run it, so its watches don't end when the manager stops
	shared := changes.New()
	manager, stop, err := test.Start(test.Conn(), broadcaster.WithBroadcaster(shared))
	require.NoError(t, err)
	assert.True(t, manager.Shared())
	assert.Same(t, shared, manager.Broadcaster())
	require.NoError(t, manager.WaitListening(context.Background()))
	ch := make(chan changeschema.Change, 1)
	done, err := shared.Watch(context.Background(), changeschema.ChangeFilter{}, ch)
	require.NoError(t, err)
	change := changeschema.Change{Action: changeschema.ChangeCreate, Schema: "shared", Table: "table"}
	require.NoError(t, manager.Publish(context.Background(), change))
	got, ok := receive(ch)
	require.True(t, ok)
	assert.Equal(t, change, got)
	require.NoError(t, stop())
	select {
	case err := <-done:
		t.Fatalf("the shared broadcaster's watch ended: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// A shared broadcaster is required
	_, err = broadcaster.New(context.Background(), test.Conn(), broadcaster.WithBroadcaster(nil))
	assert.ErrorIs(t, err, pg.ErrBadParameter)
}

func TestWatchRestart(t *testing.T) {
	// A manager which is run again, once it has stopped, can be watched as
	// soon as it's listening again, as its broadcaster runs again before
	// it listens
	manager, err := broadcaster.New(context.Background(), instantPool{test.Conn()}, broadcaster.WithChannel("test_restart"))
	require.NoError(t, err)
	for range 20 {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- manager.Run(ctx, nil) }()
		require.NoError(t, manager.WaitListening(ctx))
		watching, err := manager.Watch(context.Background(), changeschema.ChangeFilter{}, make(chan changeschema.Change, 1))
		require.NoError(t, err)
		cancel()
		require.NoError(t, <-done)
		assert.ErrorIs(t, <-watching, httpresponse.ErrServiceUnavailable)
	}
}

// instantPool is a pool which listens at once, so a manager is listening as
// soon as it runs, without waiting for the database
type instantPool struct {
	pg.PoolConn
}

func (p instantPool) With(params ...any) pg.Conn {
	return instantPool{p.PoolConn.With(params...).(pg.PoolConn)}
}

func (p instantPool) WithQueries(queries ...*pg.Queries) pg.Conn {
	return instantPool{p.PoolConn.WithQueries(queries...).(pg.PoolConn)}
}

func (p instantPool) Subscribe(ctx context.Context, channel string) (<-chan pg.Notification, error) {
	ch := make(chan pg.Notification)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}
