package broadcaster_test

import (
	"context"
	"sync"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	broadcaster "github.com/mutablelogic/go-pg/pgbroadcaster"
	schema "github.com/mutablelogic/go-pg/pgbroadcaster/schema"
	test "github.com/mutablelogic/go-pg/pgbroadcaster/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	test.Main(m)
}

func TestNew(t *testing.T) {
	// The schema and channel are the defaults, unless they're set, when they
	// must be identifiers
	ctx := context.Background()
	manager, err := broadcaster.New(ctx, test.Conn())
	require.NoError(t, err)
	assert.Equal(t, schema.DefaultSchema, manager.Schema())
	assert.Equal(t, schema.NotifyChannel, manager.Channel())

	manager, err = broadcaster.New(ctx, test.Conn(), broadcaster.WithSchema("pgbroadcaster"), broadcaster.WithChannel("test_events"))
	require.NoError(t, err)
	assert.Equal(t, "pgbroadcaster", manager.Schema())
	assert.Equal(t, "test_events", manager.Channel())

	for _, opt := range []broadcaster.Opt{broadcaster.WithSchema("not a schema"), broadcaster.WithChannel("1st")} {
		_, err := broadcaster.New(ctx, test.Conn(), opt)
		assert.ErrorIs(t, err, pg.ErrBadParameter)
	}
	_, err = broadcaster.New(ctx, nil)
	assert.ErrorIs(t, err, pg.ErrBadParameter)
}

func TestRun(t *testing.T) {
	// A manager runs until it's cancelled
	manager, err := broadcaster.New(context.Background(), test.Conn(), broadcaster.WithChannel("test_run"))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx, nil) }()
	require.Eventually(t, manager.Running, 10*time.Second, 10*time.Millisecond)
	cancel()
	assert.NoError(t, <-done)
	assert.False(t, manager.Running())
}

// flakyPool is a pool whose first listening connection is the test's, which
// it closes to make it fail, and whose next attempts to listen fail
type flakyPool struct {
	pg.PoolConn
	state *flakyState
}

type flakyState struct {
	mu       sync.Mutex
	calls    int
	failures int                  // how many attempts after the first fail
	first    chan pg.Notification // the first connection, which the test closes
	errFirst error                // the error of the first attempt, if it fails
}

func (p flakyPool) With(params ...any) pg.Conn {
	return flakyPool{p.PoolConn.With(params...).(pg.PoolConn), p.state}
}

func (p flakyPool) WithQueries(queries ...*pg.Queries) pg.Conn {
	return flakyPool{p.PoolConn.WithQueries(queries...).(pg.PoolConn), p.state}
}

func (p flakyPool) Subscribe(ctx context.Context, channel string) (<-chan pg.Notification, error) {
	p.state.mu.Lock()
	p.state.calls++
	calls := p.state.calls
	p.state.mu.Unlock()
	switch {
	case calls == 1 && p.state.errFirst != nil:
		return nil, p.state.errFirst
	case calls == 1:
		return p.state.first, nil
	case calls <= 1+p.state.failures:
		return nil, pg.ErrNotAvailable.With("database is restarting")
	default:
		return p.PoolConn.Subscribe(ctx, channel)
	}
}

func (s *flakyState) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestRunReconnect(t *testing.T) {
	// A manager whose listening connection fails, and which can't listen
	// again twice, as while the database restarts
	state := &flakyState{failures: 2, first: make(chan pg.Notification)}
	manager, err := broadcaster.New(context.Background(), flakyPool{test.Conn(), state}, broadcaster.WithChannel("test_reconnect"))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx, nil) }()
	require.NoError(t, manager.WaitListening(ctx))

	// A subscription made while it's listening
	ch := make(chan schema.Event, 1)
	_, err = manager.Subscribe(ctx, schema.EventFilter{}, ch)
	require.NoError(t, err)

	// The connection fails, and it doesn't stop, but tries to listen again,
	// until it can
	close(state.first)
	require.Eventually(t, func() bool { return state.Calls() == 4 && manager.Listening() }, 10*time.Second, 10*time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("Run returned: %v", err)
	default:
	}

	// The subscription is still sent the events
	require.NoError(t, manager.Publish(ctx, schema.Event{Schema: "reconnect", Table: "table", Action: "UPDATE"}))
	select {
	case event := <-ch:
		assert.Equal(t, schema.Event{Schema: "reconnect", Table: "table", Action: "UPDATE"}, event)
	case <-time.After(5 * time.Second):
		t.Fatal("the subscription wasn't sent the event once it listened again")
	}

	// It stops when it's cancelled
	cancel()
	assert.NoError(t, <-done)
}

func TestRunListenFails(t *testing.T) {
	// A manager which can't listen at first returns the error
	state := &flakyState{errFirst: pg.ErrNotAvailable.With("database is down")}
	manager, err := broadcaster.New(context.Background(), flakyPool{test.Conn(), state}, broadcaster.WithChannel("test_listen_fails"))
	require.NoError(t, err)
	assert.ErrorIs(t, manager.Run(context.Background(), nil), pg.ErrNotAvailable)
	assert.False(t, manager.Running())
}
