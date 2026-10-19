package httphandler_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	test "github.com/mutablelogic/go-pg/pgqueue/test"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestListTickers(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := test.IsolatedManager(t)
	server := test.NewServer(t, manager)

	// list returns the tickers for the query
	list := func(t *testing.T, query string) schema.TickerList {
		t.Helper()
		var list schema.TickerList
		require.Equal(t, http.StatusOK, server.JSON(t, http.MethodGet, "ticker"+query, nil, &list))
		return list
	}

	// A ticker, as well as the built-in tickers
	_, err := manager.RegisterTicker(ctx, "hourly", schema.TickerMeta{
		Payload:  json.RawMessage(`{"kind":"hourly"}`),
		Interval: types.Ptr(time.Hour),
	}, noopTask)
	require.NoError(t, err)

	t.Run("List", func(t *testing.T) {
		// The tickers are in name order, with their payload and interval
		result := list(t, "")
		assert.EqualValues(t, 3, result.Count)
		require.Len(t, result.Body, 3)
		names := make([]string, len(result.Body))
		for i, ticker := range result.Body {
			names[i] = ticker.Ticker
		}
		assert.Equal(t, []string{schema.DefaultCleanupTickerName, schema.DefaultMaintenanceTickerName, "hourly"}, names)

		hourly := result.Body[2]
		assert.JSONEq(t, `{"kind":"hourly"}`, string(hourly.Payload))
		require.NotNil(t, hourly.Interval)
		assert.Equal(t, time.Hour, *hourly.Interval)
		assert.Nil(t, hourly.LastAt)
	})

	t.Run("Page", func(t *testing.T) {
		result := list(t, "?offset=2&limit=1")
		assert.EqualValues(t, 3, result.Count)
		require.Len(t, result.Body, 1)
		assert.Equal(t, "hourly", result.Body[0].Ticker)
	})

	t.Run("BadQuery", func(t *testing.T) {
		status, body := server.Do(t, http.MethodGet, "ticker?offset=first", nil)
		assert.Equal(t, http.StatusBadRequest, status, "body: %s", body)
	})
}

func TestGetTicker(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := test.IsolatedManager(t)
	server := test.NewServer(t, manager)
	_, err := manager.RegisterTicker(ctx, "ticker", schema.TickerMeta{}, noopTask)
	require.NoError(t, err)

	t.Run("Found", func(t *testing.T) {
		var ticker schema.Ticker
		require.Equal(t, http.StatusOK, server.JSON(t, http.MethodGet, "ticker/ticker", nil, &ticker))
		assert.Equal(t, "ticker", ticker.Ticker)
	})

	t.Run("NotFound", func(t *testing.T) {
		assert.Equal(t, http.StatusNotFound, server.JSON(t, http.MethodGet, "ticker/missing", nil, nil))
	})

	t.Run("InvalidName", func(t *testing.T) {
		assert.Equal(t, http.StatusBadRequest, server.JSON(t, http.MethodGet, "ticker/not%20a%20name", nil, nil))
	})
}
