package httpclient_test

import (
	"encoding/json"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestListTickers(t *testing.T) {
	manager, client, ctx := newClient(t)
	_, err := manager.RegisterTicker(ctx, "hourly", schema.TickerMeta{
		Payload:  json.RawMessage(`{"kind":"hourly"}`),
		Interval: types.Ptr(time.Hour),
	}, noopTask)
	require.NoError(t, err)

	t.Run("List", func(t *testing.T) {
		// The tickers, after the built-in tickers, with their interval
		list, err := client.ListTickers(ctx, schema.TickerListRequest{})
		require.NoError(t, err)
		assert.EqualValues(t, 3, list.Count)
		require.Len(t, list.Body, 3)
		hourly := list.Body[2]
		assert.Equal(t, "hourly", hourly.Ticker)
		assert.JSONEq(t, `{"kind":"hourly"}`, string(hourly.Payload))
		require.NotNil(t, hourly.Interval)
		assert.Equal(t, time.Hour, *hourly.Interval)
	})

	t.Run("Page", func(t *testing.T) {
		list, err := client.ListTickers(ctx, schema.TickerListRequest{OffsetLimit: pg.OffsetLimit{Offset: 2, Limit: types.Ptr(uint64(1))}})
		require.NoError(t, err)
		assert.EqualValues(t, 3, list.Count)
		require.Len(t, list.Body, 1)
		assert.Equal(t, "hourly", list.Body[0].Ticker)
	})
}
