package broadcaster_test

import (
	"context"
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
