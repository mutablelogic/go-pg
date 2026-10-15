package pgqueue_test

import (
	"encoding/json"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	test "github.com/mutablelogic/go-pg/pgqueue/test"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestRegisterTicker(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)

	t.Run("Defaults", func(t *testing.T) {
		manager := test.IsolatedManager(t)
		ticker, err := manager.RegisterTicker(ctx, "defaults", schema.TickerMeta{}, noopTask)
		require.NoError(t, err)
		assert.Equal(t, "defaults", ticker.Ticker)
		assert.JSONEq(t, `{}`, string(ticker.Payload))
		require.NotNil(t, ticker.Interval)
		assert.Equal(t, time.Minute, *ticker.Interval)
		assert.Nil(t, ticker.LastAt)
	})

	t.Run("Meta", func(t *testing.T) {
		manager := test.IsolatedManager(t)
		ticker, err := manager.RegisterTicker(ctx, "meta", schema.TickerMeta{
			Payload:  json.RawMessage(`{"kind":"meta"}`),
			Interval: types.Ptr(5 * time.Minute),
		}, noopTask)
		require.NoError(t, err)
		assert.JSONEq(t, `{"kind":"meta"}`, string(ticker.Payload))
		assert.Equal(t, 5*time.Minute, *ticker.Interval)
	})

	t.Run("Existing", func(t *testing.T) {
		// A ticker which exists, such as one registered by another process,
		// is updated with the meta
		manager := test.IsolatedManager(t)
		var existing schema.Ticker
		require.NoError(t, manager.With("id", "existing").Insert(ctx, &existing, schema.TickerMeta{}))
		ticker, err := manager.RegisterTicker(ctx, "existing", schema.TickerMeta{Interval: types.Ptr(time.Hour)}, noopTask)
		require.NoError(t, err)
		assert.Equal(t, time.Hour, *ticker.Interval)
	})

	t.Run("Twice", func(t *testing.T) {
		// A ticker's callback can only be registered once, and the ticker
		// isn't changed
		manager := test.IsolatedManager(t)
		_, err := manager.RegisterTicker(ctx, "twice", schema.TickerMeta{}, noopTask)
		require.NoError(t, err)
		_, err = manager.RegisterTicker(ctx, "twice", schema.TickerMeta{Interval: types.Ptr(time.Hour)}, noopTask)
		assert.ErrorIs(t, err, httpresponse.ErrConflict)
		ticker, err := manager.GetTicker(ctx, "twice")
		require.NoError(t, err)
		assert.Equal(t, time.Minute, *ticker.Interval)
	})

	t.Run("Invalid", func(t *testing.T) {
		manager := test.IsolatedManager(t)
		_, err := manager.RegisterTicker(ctx, "no callback", schema.TickerMeta{}, noopTask)
		assert.ErrorIs(t, err, httpresponse.ErrBadRequest)
		_, err = manager.RegisterTicker(ctx, "nocallback", schema.TickerMeta{}, nil)
		assert.ErrorIs(t, err, httpresponse.ErrBadRequest)

		// Neither ticker is created, so there are only the built-in tickers
		list, err := manager.ListTickers(ctx, schema.TickerListRequest{})
		require.NoError(t, err)
		assert.EqualValues(t, 2, list.Count)
	})
}

func TestListTickers(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := test.IsolatedManager(t)
	for _, name := range []string{"two", "one", "three"} {
		_, err := manager.RegisterTicker(ctx, name, schema.TickerMeta{}, noopTask)
		require.NoError(t, err)
	}

	t.Run("All", func(t *testing.T) {
		// The tickers are in order of name, after the built-in tickers
		list, err := manager.ListTickers(ctx, schema.TickerListRequest{})
		require.NoError(t, err)
		assert.EqualValues(t, 5, list.Count)
		names := make([]string, len(list.Body))
		for i, ticker := range list.Body {
			names[i] = ticker.Ticker
		}
		assert.Equal(t, []string{schema.DefaultCleanupTickerName, schema.DefaultMaintenanceTickerName, "one", "three", "two"}, names)
	})

	t.Run("Page", func(t *testing.T) {
		list, err := manager.ListTickers(ctx, schema.TickerListRequest{OffsetLimit: pg.OffsetLimit{Offset: 3, Limit: types.Ptr(uint64(10))}})
		require.NoError(t, err)
		assert.EqualValues(t, 5, list.Count)
		require.Len(t, list.Body, 2)
		assert.Equal(t, "three", list.Body[0].Ticker)
		require.NotNil(t, list.Limit)
		assert.EqualValues(t, 2, *list.Limit)
	})
}

func TestGetUpdateDeleteTicker(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := test.IsolatedManager(t)
	_, err := manager.RegisterTicker(ctx, "ticker", schema.TickerMeta{}, noopTask)
	require.NoError(t, err)

	t.Run("Get", func(t *testing.T) {
		ticker, err := manager.GetTicker(ctx, "ticker")
		require.NoError(t, err)
		assert.Equal(t, "ticker", ticker.Ticker)

		_, err = manager.GetTicker(ctx, "missing")
		assert.ErrorIs(t, err, pg.ErrNotFound)
	})

	t.Run("Update", func(t *testing.T) {
		ticker, err := manager.UpdateTicker(ctx, "ticker", schema.TickerMeta{
			Interval: types.Ptr(9 * time.Minute),
			Payload:  json.RawMessage(`{"kind":"updated"}`),
		})
		require.NoError(t, err)
		assert.Equal(t, 9*time.Minute, *ticker.Interval)
		assert.JSONEq(t, `{"kind":"updated"}`, string(ticker.Payload))

		_, err = manager.UpdateTicker(ctx, "missing", schema.TickerMeta{Interval: types.Ptr(time.Hour)})
		assert.ErrorIs(t, err, pg.ErrNotFound)
	})

	t.Run("Delete", func(t *testing.T) {
		ticker, err := manager.DeleteTicker(ctx, "ticker")
		require.NoError(t, err)
		assert.Equal(t, "ticker", ticker.Ticker)
		_, err = manager.GetTicker(ctx, "ticker")
		assert.ErrorIs(t, err, pg.ErrNotFound)

		// The callback is removed, so the ticker can be registered again
		_, err = manager.RegisterTicker(ctx, "ticker", schema.TickerMeta{}, noopTask)
		require.NoError(t, err)

		_, err = manager.DeleteTicker(ctx, "missing")
		assert.ErrorIs(t, err, pg.ErrNotFound)
	})

	t.Run("DeleteWithoutCallback", func(t *testing.T) {
		// A ticker registered by another process has no callback in this one
		var existing schema.Ticker
		require.NoError(t, manager.With("id", "other").Insert(ctx, &existing, schema.TickerMeta{}))
		ticker, err := manager.DeleteTicker(ctx, "other")
		require.NoError(t, err)
		assert.Equal(t, "other", ticker.Ticker)
	})
}

func TestNextTicker(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := test.IsolatedManager(t)

	// The built-in tickers are due straight away, and then nothing is due
	var due []string
	for range 3 {
		next, err := manager.NextTicker(ctx)
		require.NoError(t, err)
		if next == nil {
			break
		}
		due = append(due, next.Ticker)
	}
	assert.ElementsMatch(t, []string{schema.DefaultCleanupTickerName, schema.DefaultMaintenanceTickerName}, due)

	// A new ticker is due straight away, and isn't due again until its
	// interval has passed
	_, err := manager.RegisterTicker(ctx, "hourly", schema.TickerMeta{Interval: types.Ptr(time.Hour)}, noopTask)
	require.NoError(t, err)
	next, err := manager.NextTicker(ctx)
	require.NoError(t, err)
	require.NotNil(t, next)
	assert.Equal(t, "hourly", next.Ticker)
	require.NotNil(t, next.LastAt)
	assert.WithinDuration(t, time.Now(), *next.LastAt, time.Minute)

	next, err = manager.NextTicker(ctx)
	require.NoError(t, err)
	assert.Nil(t, next)

	// A ticker without a callback in this process, such as one registered by
	// another process, isn't returned
	var other schema.Ticker
	require.NoError(t, manager.With("id", "other").Insert(ctx, &other, schema.TickerMeta{}))
	next, err = manager.NextTicker(ctx)
	require.NoError(t, err)
	assert.Nil(t, next)

	// It's due again once its interval has passed
	require.NoError(t, manager.Exec(ctx, `UPDATE ${"schema"}."ticker" SET "last_at" = NOW() - INTERVAL '2 hours' WHERE "ticker" = 'hourly'`))
	next, err = manager.NextTicker(ctx)
	require.NoError(t, err)
	require.NotNil(t, next)
	assert.Equal(t, "hourly", next.Ticker)
}

func TestTickerInterval(t *testing.T) {
	_, ctx := test.Begin(t)
	defer test.End(t)
	manager := test.IsolatedManager(t)

	// A ticker's interval must be positive
	for _, interval := range []time.Duration{0, -time.Minute} {
		_, err := manager.RegisterTicker(ctx, "invalid", schema.TickerMeta{Interval: types.Ptr(interval)}, noopTask)
		assert.ErrorIs(t, err, httpresponse.ErrBadRequest, interval)
	}
	_, err := manager.RegisterTicker(ctx, "zero", schema.TickerMeta{}, noopTask)
	require.NoError(t, err)
	_, err = manager.UpdateTicker(ctx, "zero", schema.TickerMeta{Interval: types.Ptr(time.Duration(0))})
	assert.ErrorIs(t, err, httpresponse.ErrBadRequest)

	// A ticker stored without a positive interval is never due, as it would
	// always be due
	require.NoError(t, manager.Exec(ctx, `UPDATE ${"schema"}."ticker" SET "interval" = INTERVAL '0' WHERE "ticker" = 'zero'`))
	for range 3 {
		next, err := manager.NextTicker(ctx)
		require.NoError(t, err)
		if next != nil {
			assert.NotEqual(t, "zero", next.Ticker)
		}
	}
}
