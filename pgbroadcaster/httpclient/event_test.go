package httpclient_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	httpclient "github.com/mutablelogic/go-pg/pgbroadcaster/httpclient"
	httphandler "github.com/mutablelogic/go-pg/pgbroadcaster/httphandler"
	schema "github.com/mutablelogic/go-pg/pgbroadcaster/schema"
	test "github.com/mutablelogic/go-pg/pgbroadcaster/test"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

const prefix = "/api"

func TestMain(m *testing.M) {
	test.Main(m)
}

func TestWatchEvents(t *testing.T) {
	manager, ctx := test.Begin(t)
	defer test.End(t)

	// Serve the events
	router, err := httprouter.NewRouter(context.Background(), http.NewServeMux(), prefix, "*", "broadcaster", "test")
	require.NoError(t, err)
	require.NoError(t, httphandler.RegisterHandlers(manager, router))
	server := httptest.NewServer(router)
	defer server.Close()
	client, err := httpclient.New(server.URL + prefix)
	require.NoError(t, err)

	// watch watches the events which match the filter, until the returned
	// function is called, which returns what WatchEvents returned
	watch := func(filter schema.EventFilter, fn func(schema.Event) error) (<-chan schema.Event, func() error) {
		events := make(chan schema.Event, 16)
		watchCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			done <- client.WatchEvents(watchCtx, filter, func(event schema.Event) error {
				events <- event
				if fn != nil {
					return fn(event)
				}
				return nil
			})
		}()
		return events, func() error {
			cancel()
			return <-done
		}
	}

	// publish publishes the events until one is received, as the server
	// subscribes after the client connects
	publish := func(t *testing.T, events <-chan schema.Event, publish ...schema.Event) schema.Event {
		t.Helper()
		var event schema.Event
		require.Eventually(t, func() bool {
			for _, event := range publish {
				require.NoError(t, manager.Publish(ctx, event))
			}
			select {
			case event = <-events:
				return true
			case <-time.After(100 * time.Millisecond):
				return false
			}
		}, 10*time.Second, 10*time.Millisecond)
		return event
	}

	t.Run("Filtered", func(t *testing.T) {
		// Only events which match the filter are received, and cancelling
		// the context isn't an error
		events, stop := watch(schema.EventFilter{Table: "volume", Action: "DELETE"}, nil)
		event := publish(t, events,
			schema.Event{Schema: "filer", Table: "volume", Action: "INSERT"},
			schema.Event{Schema: "filer", Table: "object", Action: "DELETE"},
			schema.Event{Schema: "filer", Table: "volume", Action: "DELETE"},
		)
		assert.Equal(t, schema.Event{Schema: "filer", Table: "volume", Action: "DELETE"}, event)
		assert.NoError(t, stop())
	})

	t.Run("Error", func(t *testing.T) {
		// An error from the function stops watching, and is returned
		errStop := errors.New("stop")
		events, stop := watch(schema.EventFilter{}, func(schema.Event) error { return errStop })
		publish(t, events, schema.Event{Schema: "filer", Table: "volume", Action: "UPDATE"})
		assert.ErrorIs(t, stop(), errStop)
	})

	t.Run("Shutdown", func(t *testing.T) {
		// When the server's broadcaster stops, watching stops, as there
		// will be no more events
		manager, stop, err := test.Start(test.Conn())
		require.NoError(t, err)
		router, err := httprouter.NewRouter(context.Background(), http.NewServeMux(), prefix, "*", "broadcaster", "test")
		require.NoError(t, err)
		require.NoError(t, httphandler.RegisterHandlers(manager, router))
		server := httptest.NewServer(router)
		defer server.Close()
		client, err := httpclient.New(server.URL + prefix)
		require.NoError(t, err)

		done := make(chan error, 1)
		go func() {
			done <- client.WatchEvents(ctx, schema.EventFilter{}, func(schema.Event) error { return nil })
		}()
		require.Eventually(t, func() bool { return manager.Subscribers() == 1 }, 5*time.Second, 10*time.Millisecond)
		require.NoError(t, stop())
		select {
		case err := <-done:
			assert.ErrorIs(t, err, pg.ErrNotAvailable)
		case <-time.After(5 * time.Second):
			t.Fatal("watching didn't stop when the server's broadcaster stopped")
		}
	})

	t.Run("NoFunction", func(t *testing.T) {
		assert.Error(t, client.WatchEvents(ctx, schema.EventFilter{}, nil))
	})
}
