package broadcaster_test

import (
	"context"
	"strings"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgbroadcaster/schema"
	test "github.com/mutablelogic/go-pg/pgbroadcaster/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestPublish(t *testing.T) {
	// A published event is logged by the manager listening on its channel,
	// and only one run of a manager listens at a time
	manager, ctx := test.Begin(t)
	defer test.End(t)
	assert.True(t, manager.Running())
	assert.ErrorIs(t, manager.Run(ctx, nil), pg.ErrConflict)

	require.NoError(t, manager.Publish(ctx, schema.Event{Schema: "filer", Table: "volume", Action: "UPDATE"}))
	require.Eventually(t, func() bool {
		return strings.Contains(test.Logs(), `payload="{\"schema\":\"filer\",\"table\":\"volume\",\"action\":\"UPDATE\"}"`)
	}, 10*time.Second, 10*time.Millisecond, test.Logs())

	// An event which is too large isn't published
	err := manager.Publish(ctx, schema.Event{Table: strings.Repeat("x", schema.MaxPayloadSize)})
	assert.ErrorIs(t, err, pg.ErrBadParameter)
}

func TestSubscribe(t *testing.T) {
	manager, ctx := test.Begin(t)
	defer test.End(t)

	// subscribe subscribes a channel to the events which match the filter,
	// until the returned function is called
	subscribe := func(filter schema.EventFilter, size int) (chan schema.Event, func()) {
		t.Helper()
		ch := make(chan schema.Event, size)
		subCtx, cancel := context.WithCancel(ctx)
		done, err := manager.Subscribe(subCtx, filter, ch)
		require.NoError(t, err)
		return ch, func() {
			cancel()
			assert.NoError(t, <-done)
		}
	}
	receive := func(ch chan schema.Event) (schema.Event, bool) {
		select {
		case event := <-ch:
			return event, true
		case <-time.After(5 * time.Second):
			return schema.Event{}, false
		}
	}

	// Every subscriber is sent an event which is published
	first, unsubscribeFirst := subscribe(schema.EventFilter{}, 1)
	second, unsubscribeSecond := subscribe(schema.EventFilter{}, 1)
	defer unsubscribeSecond()
	volumes, unsubscribeVolumes := subscribe(schema.EventFilter{Schema: "filer", Table: "volume"}, 1)
	defer unsubscribeVolumes()
	time.Sleep(50 * time.Millisecond)
	event := schema.Event{Schema: "filer", Table: "object", Action: "INSERT"}
	require.NoError(t, manager.Publish(ctx, event))
	for _, ch := range []chan schema.Event{first, second} {
		got, ok := receive(ch)
		require.True(t, ok, "the event wasn't sent")
		assert.Equal(t, event, got)
	}

	// A subscriber is only sent the events which match its filter
	require.NoError(t, manager.Publish(ctx, schema.Event{Schema: "filer", Table: "volume", Action: "UPDATE"}))
	got, ok := receive(volumes)
	require.True(t, ok, "the event which matches wasn't sent")
	assert.Equal(t, "volume", got.Table)
	for _, ch := range []chan schema.Event{first, second} {
		_, ok := receive(ch)
		require.True(t, ok)
	}

	// A subscriber which has unsubscribed isn't sent events, and one which
	// isn't ready for one misses it, which is logged
	unsubscribeFirst()
	full, unsubscribeFull := subscribe(schema.EventFilter{}, 0)
	defer unsubscribeFull()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, manager.Publish(ctx, schema.Event{Table: "volume"}))
	got, ok = receive(second)
	require.True(t, ok)
	assert.Equal(t, "volume", got.Table)
	select {
	case <-first:
		t.Error("an unsubscribed channel was sent an event")
	case <-full:
		t.Error("a channel which wasn't ready was sent an event")
	case event := <-volumes:
		t.Errorf("an event which doesn't match the filter was sent: %v", event)
	default:
	}
	require.Eventually(t, func() bool { return strings.Contains(test.Logs(), "subscribers missed an event") }, 5*time.Second, 10*time.Millisecond)

	// A notification which isn't an event isn't sent, and is logged
	require.NoError(t, test.Conn().With("channel", manager.Channel()).Exec(ctx, `SELECT pg_notify(@channel, 'not json')`))
	require.Eventually(t, func() bool { return strings.Contains(test.Logs(), "not an event") }, 5*time.Second, 10*time.Millisecond)
	select {
	case event := <-second:
		t.Errorf("a notification which isn't an event was sent: %v", event)
	default:
	}

	// Once a subscription has ended, it's no longer subscribed, so its
	// channel can be closed, and nothing more is sent to it
	before := manager.Subscribers()
	ch := make(chan schema.Event, 1)
	subCtx, cancel := context.WithCancel(ctx)
	done, err := manager.Subscribe(subCtx, schema.EventFilter{}, ch)
	require.NoError(t, err)
	assert.Equal(t, before+1, manager.Subscribers())
	cancel()
	require.NoError(t, <-done)
	assert.Equal(t, before, manager.Subscribers())
	close(ch)

	// A channel is required
	_, err = manager.Subscribe(ctx, schema.EventFilter{}, nil)
	assert.ErrorIs(t, err, pg.ErrBadParameter)
}

func TestSubscribeStopped(t *testing.T) {
	// A manager of its own, which is stopped
	manager, stop, err := test.Start(test.Conn())
	require.NoError(t, err)

	// It's listening, as it's started once it is
	require.NoError(t, manager.WaitListening(context.Background()))

	// A subscription is made before Subscribe returns, and ends when the
	// manager stops, as there will be no more events
	done, err := manager.Subscribe(context.Background(), schema.EventFilter{}, make(chan schema.Event, 1))
	require.NoError(t, err)
	assert.Equal(t, 1, manager.Subscribers())
	require.NoError(t, stop())
	select {
	case err := <-done:
		assert.ErrorIs(t, err, pg.ErrNotAvailable)
	case <-time.After(5 * time.Second):
		t.Fatal("subscription didn't end when the manager stopped")
	}
	assert.Zero(t, manager.Subscribers())

	// A subscription isn't made once the manager has stopped
	late, err := manager.Subscribe(context.Background(), schema.EventFilter{}, make(chan schema.Event, 1))
	assert.ErrorIs(t, err, pg.ErrNotAvailable)
	assert.Nil(t, late)
	assert.Zero(t, manager.Subscribers())

	// A manager which isn't listening isn't waited for, once the context
	// is cancelled
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, manager.WaitListening(ctx), context.DeadlineExceeded)
}

func TestSubscribeTwice(t *testing.T) {
	manager, ctx := test.Begin(t)
	defer test.End(t)

	// A channel is subscribed once at a time
	ch := make(chan schema.Event, 1)
	firstCtx, cancelFirst := context.WithCancel(ctx)
	first, err := manager.Subscribe(firstCtx, schema.EventFilter{}, ch)
	require.NoError(t, err)
	before := manager.Subscribers()
	second, err := manager.Subscribe(ctx, schema.EventFilter{}, ch)
	assert.ErrorIs(t, err, pg.ErrConflict)
	assert.Nil(t, second)
	assert.Equal(t, before, manager.Subscribers())

	// The first subscription is still sent the events
	require.NoError(t, manager.Publish(ctx, schema.Event{Schema: "twice", Table: "first", Action: "INSERT"}))
	select {
	case event := <-ch:
		assert.Equal(t, schema.Event{Schema: "twice", Table: "first", Action: "INSERT"}, event)
	case <-time.After(5 * time.Second):
		t.Fatal("the first subscription wasn't sent the event")
	}

	// Once it ends, the channel can be subscribed again
	cancelFirst()
	select {
	case err := <-first:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the first subscription didn't end")
	}
	againCtx, cancelAgain := context.WithCancel(ctx)
	defer cancelAgain()
	_, err = manager.Subscribe(againCtx, schema.EventFilter{}, ch)
	assert.NoError(t, err)
}
