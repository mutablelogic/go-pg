package httphandler_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	// Packages
	httphandler "github.com/mutablelogic/go-pg/pgbroadcaster/httphandler"
	schema "github.com/mutablelogic/go-pg/pgbroadcaster/schema"
	test "github.com/mutablelogic/go-pg/pgbroadcaster/test"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

const prefix = "/api"

func TestMain(m *testing.M) {
	test.Main(m)
}

func TestEventWatch(t *testing.T) {
	manager, ctx := test.Begin(t)
	defer test.End(t)

	// Serve the events
	router, err := httprouter.NewRouter(context.Background(), http.NewServeMux(), prefix, "*", "broadcaster", "test")
	require.NoError(t, err)
	require.NoError(t, httphandler.RegisterHandlers(manager, router))
	server := httptest.NewServer(router)
	defer server.Close()

	// watch requests the events which match the query, and returns the data
	// of the events which are streamed
	watch := func(t *testing.T, query, accept string) (*http.Response, <-chan schema.Event) {
		t.Helper()
		watchCtx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		req, err := http.NewRequestWithContext(watchCtx, http.MethodGet, server.URL+prefix+"/event?"+query, nil)
		require.NoError(t, err)
		req.Header.Set("Accept", accept)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		t.Cleanup(func() { resp.Body.Close() })
		events := make(chan schema.Event, 16)
		go func() {
			defer close(events)
			var name string
			scanner := bufio.NewScanner(resp.Body)
			for scanner.Scan() {
				line := scanner.Text()
				if value, ok := strings.CutPrefix(line, "event: "); ok {
					name = value
				} else if value, ok := strings.CutPrefix(line, "data: "); ok && name == schema.EventName {
					var event schema.Event
					if json.Unmarshal([]byte(value), &event) == nil {
						events <- event
					}
				}
			}
		}()
		return resp, events
	}

	t.Run("Filtered", func(t *testing.T) {
		// Only events which match the query are streamed, and an event is
		// published until one arrives, as the handler subscribes after the
		// client connects
		resp, events := watch(t, "table=volume", types.ContentTypeTextStream)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, types.ContentTypeTextStream, resp.Header.Get(types.ContentTypeHeader))

		var event schema.Event
		require.Eventually(t, func() bool {
			require.NoError(t, manager.Publish(ctx, schema.Event{Schema: "filer", Table: "object", Action: "INSERT"}))
			require.NoError(t, manager.Publish(ctx, schema.Event{Schema: "filer", Table: "volume", Action: "UPDATE"}))
			select {
			case event = <-events:
				return true
			case <-time.After(100 * time.Millisecond):
				return false
			}
		}, 10*time.Second, 10*time.Millisecond)
		assert.Equal(t, schema.Event{Schema: "filer", Table: "volume", Action: "UPDATE"}, event)
	})

	t.Run("Shutdown", func(t *testing.T) {
		// When the broadcaster stops, a shutdown event is sent, and the
		// stream ends, so a server isn't waiting for it to stop
		manager, stop, err := test.Start(test.Conn())
		require.NoError(t, err)
		router, err := httprouter.NewRouter(context.Background(), http.NewServeMux(), prefix, "*", "broadcaster", "test")
		require.NoError(t, err)
		require.NoError(t, httphandler.RegisterHandlers(manager, router))
		server := httptest.NewServer(router)
		defer server.Close()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+prefix+"/event", nil)
		require.NoError(t, err)
		req.Header.Set("Accept", types.ContentTypeTextStream)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Eventually(t, func() bool { return manager.Subscribers() == 1 }, 5*time.Second, 10*time.Millisecond)
		require.NoError(t, stop())

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Contains(t, string(body), "event: "+schema.EventShutdown+"\n")

		// Once it's stopped, a client which watches again isn't streamed to
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, server.URL+prefix+"/event", nil)
		require.NoError(t, err)
		req.Header.Set("Accept", types.ContentTypeTextStream)
		late, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer late.Body.Close()
		assert.Equal(t, http.StatusServiceUnavailable, late.StatusCode)
		assert.True(t, late.Close, "expected the connection to be closed")
		assert.Zero(t, manager.Subscribers())
	})

	t.Run("WriteTimeout", func(t *testing.T) {
		// A stream isn't cut off by the server's write timeout, which is for
		// the whole response, so an event published after it is received
		server := httptest.NewUnstartedServer(router)
		server.Config.WriteTimeout = 200 * time.Millisecond
		server.Start()
		defer server.Close()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+prefix+"/event?table=timeout", nil)
		require.NoError(t, err)
		req.Header.Set("Accept", types.ContentTypeTextStream)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "no", resp.Header.Get("X-Accel-Buffering"))

		// Read the events after the timeout
		events := make(chan schema.Event, 1)
		go func() {
			scanner := bufio.NewScanner(resp.Body)
			for scanner.Scan() {
				if value, ok := strings.CutPrefix(scanner.Text(), "data: "); ok {
					var event schema.Event
					if json.Unmarshal([]byte(value), &event) == nil && event.Table == "timeout" {
						events <- event
						return
					}
				}
			}
		}()
		time.Sleep(500 * time.Millisecond)
		require.NoError(t, manager.Publish(ctx, schema.Event{Schema: "test", Table: "timeout", Action: "UPDATE"}))
		select {
		case event := <-events:
			assert.Equal(t, "UPDATE", event.Action)
		case <-time.After(5 * time.Second):
			t.Fatal("the stream was cut off by the write timeout")
		}
	})

	t.Run("NotAcceptable", func(t *testing.T) {
		resp, _ := watch(t, "", types.ContentTypeJSON)
		assert.Equal(t, http.StatusNotAcceptable, resp.StatusCode)
	})
}
