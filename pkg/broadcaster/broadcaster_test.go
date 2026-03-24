package broadcaster

import (
	"context"
	"fmt"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	test "github.com/mutablelogic/go-pg/pkg/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

var conn test.Conn

///////////////////////////////////////////////////////////////////////////////
// TEST MAIN

func TestMain(m *testing.M) {
	test.Main(m, &conn)
}

///////////////////////////////////////////////////////////////////////////////
// HELPERS

func newTestBroadcaster(t *testing.T) *broadcaster {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	b := &broadcaster{
		ctx:    ctx,
		cancel: cancel,
	}
	t.Cleanup(b.Close)

	return b
}

func waitForSubscriberCount(t *testing.T, b *broadcaster, want int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		b.mu.RLock()
		got := len(b.subscribers)
		b.mu.RUnlock()

		if got == want {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	b.mu.RLock()
	got := len(b.subscribers)
	b.mu.RUnlock()
	t.Fatalf("subscriber count = %d, want %d", got, want)
}

///////////////////////////////////////////////////////////////////////////////
// UNIT TESTS

func TestChangeNotification_String(t *testing.T) {
	assert := assert.New(t)

	value := ChangeNotification{
		Schema: "auth",
		Table:  "user",
		Action: "INSERT",
	}

	assert.Equal("{\n  \"schema\": \"auth\",\n  \"table\": \"user\",\n  \"action\": \"INSERT\"\n}", value.String())
}

func TestBroadcaster_SubscribeAndBroadcast(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	b := newTestBroadcaster(t)

	first := make(chan ChangeNotification, 1)
	second := make(chan ChangeNotification, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(b.Subscribe(ctx, func(change ChangeNotification) {
		first <- change
	}))
	require.NoError(b.Subscribe(ctx, func(change ChangeNotification) {
		second <- change
	}))

	change := ChangeNotification{Schema: "public", Table: "widget", Action: "UPDATE"}
	b.broadcast(change)

	select {
	case got := <-first:
		assert.Equal(change, got)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first subscriber")
	}

	select {
	case got := <-second:
		assert.Equal(change, got)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for second subscriber")
	}
}

func TestBroadcaster_SubscribeNilCallback(t *testing.T) {
	require := require.New(t)
	b := newTestBroadcaster(t)

	err := b.Subscribe(context.Background(), nil)
	require.ErrorIs(err, pg.ErrBadParameter)
}

func TestBroadcaster_UnsubscribeRemovesCanceledSubscriber(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	b := newTestBroadcaster(t)

	staleCtx, staleCancel := context.WithCancel(context.Background())
	activeCtx, activeCancel := context.WithCancel(context.Background())
	defer activeCancel()

	active := make(chan ChangeNotification, 1)

	require.NoError(b.Subscribe(staleCtx, func(ChangeNotification) {}))
	require.NoError(b.Subscribe(activeCtx, func(change ChangeNotification) {
		active <- change
	}))
	waitForSubscriberCount(t, b, 2)

	staleCancel()
	waitForSubscriberCount(t, b, 1)

	change := ChangeNotification{Schema: "public", Table: "widget", Action: "DELETE"}
	b.broadcast(change)

	select {
	case got := <-active:
		assert.Equal(change, got)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for active subscriber")
	}
}

///////////////////////////////////////////////////////////////////////////////
// INTEGRATION TESTS

func TestBroadcaster_Integration_DeliversDecodedChange(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	channel := fmt.Sprintf("test_broadcaster_delivers_%d", time.Now().UnixNano())
	b, err := NewBroadcaster(conn, channel)
	require.NoError(err)
	defer b.Close()

	changes := make(chan ChangeNotification, 1)
	require.NoError(b.Subscribe(ctx, func(change ChangeNotification) {
		select {
		case changes <- change:
		default:
		}
	}))

	payload := `{"schema":"auth","table":"user","action":"INSERT"}`
	require.NoError(conn.Exec(context.Background(), fmt.Sprintf("SELECT pg_notify('%s', '%s')", channel, payload)))

	select {
	case change := <-changes:
		assert.Equal("auth", change.Schema)
		assert.Equal("user", change.Table)
		assert.Equal("INSERT", change.Action)
	case <-ctx.Done():
		t.Fatal("timed out waiting for notification")
	}
}

func TestBroadcaster_Integration_BroadcastsToAllSubscribers(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	channel := fmt.Sprintf("test_broadcaster_broadcasts_%d", time.Now().UnixNano())
	b, err := NewBroadcaster(conn, channel)
	require.NoError(err)
	defer b.Close()

	first := make(chan ChangeNotification, 1)
	second := make(chan ChangeNotification, 1)

	require.NoError(b.Subscribe(ctx, func(change ChangeNotification) {
		select {
		case first <- change:
		default:
		}
	}))
	require.NoError(b.Subscribe(ctx, func(change ChangeNotification) {
		select {
		case second <- change:
		default:
		}
	}))

	payload := `{"schema":"auth","table":"group","action":"UPDATE"}`
	require.NoError(conn.Exec(context.Background(), fmt.Sprintf("SELECT pg_notify('%s', '%s')", channel, payload)))

	select {
	case change := <-first:
		assert.Equal("group", change.Table)
		assert.Equal("UPDATE", change.Action)
	case <-ctx.Done():
		t.Fatal("timed out waiting for first subscriber")
	}

	select {
	case change := <-second:
		assert.Equal("group", change.Table)
		assert.Equal("UPDATE", change.Action)
	case <-ctx.Done():
		t.Fatal("timed out waiting for second subscriber")
	}
}

func TestBroadcaster_Integration_IgnoresInvalidJSON(t *testing.T) {
	require := require.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()

	channel := fmt.Sprintf("test_broadcaster_invalid_%d", time.Now().UnixNano())
	b, err := NewBroadcaster(conn, channel)
	require.NoError(err)
	defer b.Close()

	changes := make(chan ChangeNotification, 1)
	require.NoError(b.Subscribe(ctx, func(change ChangeNotification) {
		select {
		case changes <- change:
		default:
		}
	}))

	require.NoError(conn.Exec(context.Background(), fmt.Sprintf("SELECT pg_notify('%s', 'not-json')", channel)))

	select {
	case change := <-changes:
		t.Fatalf("unexpected change received: %v", change)
	case <-ctx.Done():
	}
}
