package manager_test

import (
	"errors"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	test "github.com/mutablelogic/go-pg/pgqueue/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestRegisterTickerDefaults(t *testing.T) {
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	ticker, err := mgr.RegisterTicker(ctx, "ticker_defaults", schema.TickerMeta{}, noopTask)
	require.NoError(t, err)
	require.NotNil(t, ticker)
	assert.Equal(t, "ticker_defaults", ticker.Ticker)
	assert.JSONEq(t, `{}`, string(ticker.Payload))
	if assert.NotNil(t, ticker.Interval) {
		assert.Equal(t, time.Minute, *ticker.Interval)
	}
	assert.Nil(t, ticker.LastAt)

	_, _ = mgr.DeleteTicker(ctx, ticker.Ticker)
}

func TestRegisterTickerWithPatch(t *testing.T) {
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	interval := 5 * time.Minute
	payload := []byte(`{"kind":"patched"}`)
	ticker, err := mgr.RegisterTicker(ctx, "ticker_patch", schema.TickerMeta{Payload: payload, Interval: &interval}, noopTask)
	require.NoError(t, err)
	require.NotNil(t, ticker)
	assert.Equal(t, "ticker_patch", ticker.Ticker)
	assert.JSONEq(t, string(payload), string(ticker.Payload))
	if assert.NotNil(t, ticker.Interval) {
		assert.Equal(t, interval, *ticker.Interval)
	}

	_, _ = mgr.DeleteTicker(ctx, ticker.Ticker)
}

func TestListTickers(t *testing.T) {
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	ticker1, err := mgr.RegisterTicker(ctx, "ticker_list_one", schema.TickerMeta{}, noopTask)
	require.NoError(t, err)
	ticker2, err := mgr.RegisterTicker(ctx, "ticker_list_two", schema.TickerMeta{}, noopTask)
	require.NoError(t, err)
	defer func() {
		_, _ = mgr.DeleteTicker(ctx, ticker1.Ticker)
		_, _ = mgr.DeleteTicker(ctx, ticker2.Ticker)
	}()

	limit := uint64(10)
	result, err := mgr.ListTickers(ctx, schema.TickerListRequest{OffsetLimit: pg.OffsetLimit{Limit: &limit}})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.GreaterOrEqual(t, result.Count, uint64(2))
	assert.GreaterOrEqual(t, len(result.Body), 2)
	if assert.NotNil(t, result.Limit) {
		assert.Equal(t, min(limit, result.Count), *result.Limit)
	}

	names := make([]string, 0, len(result.Body))
	for _, ticker := range result.Body {
		names = append(names, ticker.Ticker)
	}
	assert.Contains(t, names, ticker1.Ticker)
	assert.Contains(t, names, ticker2.Ticker)
}

func TestGetUpdateDeleteTicker(t *testing.T) {
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	ticker, err := mgr.RegisterTicker(ctx, "ticker_get_update_delete", schema.TickerMeta{}, noopTask)
	require.NoError(t, err)

	fetched, err := mgr.GetTicker(ctx, ticker.Ticker)
	require.NoError(t, err)
	require.NotNil(t, fetched)
	assert.Equal(t, ticker.Ticker, fetched.Ticker)

	interval := 9 * time.Minute
	payload := []byte(`{"kind":"updated"}`)
	updated, err := mgr.UpdateTicker(ctx, ticker.Ticker, schema.TickerMeta{Interval: &interval, Payload: payload})
	require.NoError(t, err)
	require.NotNil(t, updated)
	assert.JSONEq(t, string(payload), string(updated.Payload))
	if assert.NotNil(t, updated.Interval) {
		assert.Equal(t, interval, *updated.Interval)
	}

	deleted, err := mgr.DeleteTicker(ctx, ticker.Ticker)
	require.NoError(t, err)
	require.NotNil(t, deleted)
	assert.Equal(t, ticker.Ticker, deleted.Ticker)

	_, err = mgr.GetTicker(ctx, ticker.Ticker)
	require.Error(t, err)
	assert.True(t, errors.Is(err, pg.ErrNotFound))
}

func TestNextTicker(t *testing.T) {
	mgr, ctx := test.Begin(t)
	defer test.End(t)

	interval := time.Hour
	ticker, err := mgr.RegisterTicker(ctx, "ticker_next", schema.TickerMeta{Interval: &interval}, noopTask)
	require.NoError(t, err)
	defer func() {
		_, _ = mgr.DeleteTicker(ctx, ticker.Ticker)
	}()

	var next *schema.Ticker
	found := false

	for i := 0; i < 10; i++ {
		next, err = mgr.NextTicker(ctx)
		require.NoError(t, err)
		require.NotNil(t, next)
		if next.Ticker == ticker.Ticker {
			found = true
			break
		}
	}

	require.True(t, found, "expected to see ticker_next among matured tickers")
	assert.NotNil(t, next.LastAt)

	for i := 0; i < 10; i++ {
		next, err = mgr.NextTicker(ctx)
		require.NoError(t, err)
		if next == nil {
			break
		}
	}
	assert.Nil(t, next)
}
