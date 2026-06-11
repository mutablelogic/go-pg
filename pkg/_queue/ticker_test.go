package queue_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	assert "github.com/stretchr/testify/assert"
)

////////////////////////////////////////////////////////////////////////////////
// TICKER CRUD TESTS

func Test_Ticker_RegisterTicker(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_ticker_register"))
	assert.NoError(err)
	assert.NotNil(mgr)

	t.Run("CreateNewTicker", func(t *testing.T) {
		interval := 5 * time.Minute
		payload := json.RawMessage(`{"key":"value"}`)

		ticker, err := mgr.RegisterTicker(ctx, schema.TickerMeta{
			Ticker:   "test-ticker",
			Interval: &interval,
			Payload:  payload,
		})

		assert.NoError(err)
		assert.NotNil(ticker)
		assert.Equal("test-ticker", ticker.Ticker)
		assert.Equal("test_ticker_register", ticker.Namespace)
		assert.NotNil(ticker.Interval)
		assert.Equal(interval, *ticker.Interval)
		assert.JSONEq(string(payload), string(ticker.Payload))
	})

	t.Run("UpdateExistingTicker", func(t *testing.T) {
		// Create initial ticker
		interval1 := 3 * time.Minute
		ticker1, err := mgr.RegisterTicker(ctx, schema.TickerMeta{
			Ticker:   "update-ticker",
			Interval: &interval1,
		})
		assert.NoError(err)
		assert.Equal(interval1, *ticker1.Interval)

		// Update the ticker
		interval2 := 10 * time.Minute
		payload := json.RawMessage(`{"updated":true}`)
		ticker2, err := mgr.RegisterTicker(ctx, schema.TickerMeta{
			Ticker:   "update-ticker",
			Interval: &interval2,
			Payload:  payload,
		})

		assert.NoError(err)
		assert.Equal("update-ticker", ticker2.Ticker)
		assert.Equal(interval2, *ticker2.Interval)
		assert.JSONEq(string(payload), string(ticker2.Payload))
	})

	t.Run("InvalidTickerName", func(t *testing.T) {
		_, err := mgr.RegisterTicker(ctx, schema.TickerMeta{
			Ticker: "invalid ticker!", // Space is not allowed
		})
		assert.Error(err)
	})

	t.Run("EmptyTickerName", func(t *testing.T) {
		_, err := mgr.RegisterTicker(ctx, schema.TickerMeta{
			Ticker: "",
		})
		assert.Error(err)
	})
}

func Test_Ticker_ListTickers(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_ticker_list"))
	assert.NoError(err)

	// Create some tickers
	for i := 1; i <= 3; i++ {
		interval := time.Duration(i) * time.Minute
		_, err := mgr.RegisterTicker(ctx, schema.TickerMeta{
			Ticker:   "ticker-" + string(rune('0'+i)),
			Interval: &interval,
		})
		assert.NoError(err)
	}

	t.Run("ListAll", func(t *testing.T) {
		list, err := mgr.ListTickers(ctx, schema.TickerListRequest{})
		assert.NoError(err)
		assert.NotNil(list)
		assert.GreaterOrEqual(len(list.Body), 3)
	})

	t.Run("ListWithLimit", func(t *testing.T) {
		limit := uint64(2)
		list, err := mgr.ListTickers(ctx, schema.TickerListRequest{
			OffsetLimit: pg.OffsetLimit{
				Limit: &limit,
			},
		})
		assert.NoError(err)
		assert.NotNil(list)
		assert.LessOrEqual(len(list.Body), 2)
	})
}

func Test_Ticker_GetTicker(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_ticker_get"))
	assert.NoError(err)

	// Create a ticker
	interval := 7 * time.Minute
	payload := json.RawMessage(`{"test":"data"}`)
	_, err = mgr.RegisterTicker(ctx, schema.TickerMeta{
		Ticker:   "get-ticker",
		Interval: &interval,
		Payload:  payload,
	})
	assert.NoError(err)

	t.Run("GetExisting", func(t *testing.T) {
		ticker, err := mgr.GetTicker(ctx, "get-ticker")
		assert.NoError(err)
		assert.NotNil(ticker)
		assert.Equal("get-ticker", ticker.Ticker)
		assert.Equal("test_ticker_get", ticker.Namespace)
		assert.Equal(interval, *ticker.Interval)
		assert.JSONEq(string(payload), string(ticker.Payload))
	})

	t.Run("GetNonExistent", func(t *testing.T) {
		_, err := mgr.GetTicker(ctx, "nonexistent")
		assert.Error(err)
	})
}

func Test_Ticker_DeleteTicker(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_ticker_delete"))
	assert.NoError(err)

	// Create a ticker
	_, err = mgr.RegisterTicker(ctx, schema.TickerMeta{
		Ticker: "delete-ticker",
	})
	assert.NoError(err)

	t.Run("DeleteExisting", func(t *testing.T) {
		ticker, err := mgr.DeleteTicker(ctx, "delete-ticker")
		assert.NoError(err)
		assert.NotNil(ticker)
		assert.Equal("delete-ticker", ticker.Ticker)

		// Verify it's deleted
		_, err = mgr.GetTicker(ctx, "delete-ticker")
		assert.Error(err)
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {
		_, err := mgr.DeleteTicker(ctx, "nonexistent")
		assert.Error(err)
	})
}

func Test_Ticker_UpdateTicker(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_ticker_update"))
	assert.NoError(err)

	// Create a ticker
	interval := 2 * time.Minute
	_, err = mgr.RegisterTicker(ctx, schema.TickerMeta{
		Ticker:   "update-ticker",
		Interval: &interval,
	})
	assert.NoError(err)

	t.Run("UpdateExisting", func(t *testing.T) {
		newInterval := 15 * time.Minute
		newPayload := json.RawMessage(`{"status":"updated"}`)

		ticker, err := mgr.UpdateTicker(ctx, "update-ticker", schema.TickerMeta{
			Interval: &newInterval,
			Payload:  newPayload,
		})

		assert.NoError(err)
		assert.NotNil(ticker)
		assert.Equal("update-ticker", ticker.Ticker)
		assert.Equal(newInterval, *ticker.Interval)
		assert.JSONEq(string(newPayload), string(ticker.Payload))
	})

	t.Run("UpdateNonExistent", func(t *testing.T) {
		newInterval := 5 * time.Minute
		_, err := mgr.UpdateTicker(ctx, "nonexistent", schema.TickerMeta{
			Interval: &newInterval,
		})
		assert.Error(err)
	})

	t.Run("UpdateWithNoChanges", func(t *testing.T) {
		_, err := mgr.UpdateTicker(ctx, "update-ticker", schema.TickerMeta{})
		assert.Error(err) // Should fail with "No patch values"
	})
}

func Test_Ticker_NextTicker(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_ticker_next"))
	assert.NoError(err)

	t.Run("GetMaturedTicker", func(t *testing.T) {
		// Create a ticker with very short interval
		interval := 100 * time.Millisecond
		_, err := mgr.RegisterTicker(ctx, schema.TickerMeta{
			Ticker:   "fast-ticker",
			Interval: &interval,
		})
		assert.NoError(err)

		// Get the ticker (this will set ts to NOW)
		ticker1, err := mgr.NextTicker(ctx)
		assert.NoError(err)
		assert.NotNil(ticker1)
		assert.Equal("fast-ticker", ticker1.Ticker)

		// Wait for it to mature
		time.Sleep(150 * time.Millisecond)

		ticker2, err := mgr.NextTicker(ctx)
		assert.NoError(err)
		assert.NotNil(ticker2)
		assert.Equal("fast-ticker", ticker2.Ticker)
	})
}

func Test_Ticker_NextTickerNs(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	// Create manager with default namespace
	mgr, err := queue.New(ctx, conn, queue.WithNamespace("default_ns"))
	assert.NoError(err)

	// Register ticker in specific namespace
	interval := 1 * time.Minute
	ticker1, err := mgr.RegisterTickerNs(ctx, "custom_ns", schema.TickerMeta{
		Ticker:   "ns-ticker",
		Interval: &interval,
	})
	assert.NoError(err)
	assert.Equal("custom_ns", ticker1.Namespace)

	t.Run("GetTickerFromNamespace", func(t *testing.T) {
		ticker, err := mgr.NextTickerNs(ctx, "custom_ns")
		assert.NoError(err)
		assert.NotNil(ticker)
		assert.Equal("ns-ticker", ticker.Ticker)
		assert.Equal("custom_ns", ticker.Namespace)
	})

	t.Run("GetTickerFromWrongNamespace", func(t *testing.T) {
		ticker, err := mgr.NextTickerNs(ctx, "wrong_ns")
		assert.NoError(err)
		assert.Nil(ticker) // No tickers in wrong_ns
	})
}

func Test_Ticker_RunTickerLoop(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("ticker_ns"))
	assert.NoError(err)

	t.Run("TickerFiresCorrectly", func(t *testing.T) {
		// Register a ticker that will mature quickly
		interval := 200 * time.Millisecond
		payload := json.RawMessage(`{"test":"fire"}`)
		ticker, err := mgr.RegisterTickerNs(ctx, "ticker_ns", schema.TickerMeta{
			Ticker:   "fast-ticker",
			Interval: &interval,
			Payload:  payload,
		})
		assert.NoError(err)
		assert.NotNil(ticker)

		// Start RunTickerLoopNsChan
		tickerChan := make(chan *schema.Ticker, 10)
		loopCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		go mgr.RunTickerLoopNsChan(loopCtx, "ticker_ns", tickerChan, 100*time.Millisecond)

		// Wait for ticker to fire (it needs to mature first)
		select {
		case receivedTicker := <-tickerChan:
			assert.NotNil(receivedTicker)
			assert.Equal("fast-ticker", receivedTicker.Ticker)
			assert.Equal("ticker_ns", receivedTicker.Namespace)
			assert.JSONEq(string(payload), string(receivedTicker.Payload))
		case <-time.After(2 * time.Second):
			t.Fatal("Ticker did not fire within timeout")
		}

		// Clean up
		_, err = mgr.DeleteTicker(ctx, ticker.Ticker)
		assert.NoError(err)
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		// Register a ticker
		interval := 200 * time.Millisecond
		ticker, err := mgr.RegisterTickerNs(ctx, "ticker_ns", schema.TickerMeta{
			Ticker:   "cancel-ticker",
			Interval: &interval,
		})
		assert.NoError(err)

		// Start RunTickerLoopNsChan
		tickerChan := make(chan *schema.Ticker, 10)
		loopCtx, cancel := context.WithCancel(ctx)

		go mgr.RunTickerLoopNsChan(loopCtx, "ticker_ns", tickerChan, 100*time.Millisecond)

		// Wait for first fire
		select {
		case <-tickerChan:
			// Ticker fired successfully
		case <-time.After(2 * time.Second):
			t.Fatal("Ticker did not fire initially")
		}

		// Cancel context
		cancel()

		// Give some time for the loop to exit
		time.Sleep(200 * time.Millisecond)

		// Verify loop stopped - channel should not receive more items
		// even though ticker would mature again after its interval
		select {
		case <-tickerChan:
			t.Fatal("Received ticker after context cancellation")
		case <-time.After(500 * time.Millisecond):
			// Good - no tickers received
		}

		// Clean up
		_, err = mgr.DeleteTicker(ctx, ticker.Ticker)
		assert.NoError(err)
	})

	t.Run("CountMultipleFires", func(t *testing.T) {
		// Register a ticker with a short interval
		interval := 250 * time.Millisecond
		ticker, err := mgr.RegisterTickerNs(ctx, "ticker_ns", schema.TickerMeta{
			Ticker:   "count-ticker",
			Interval: &interval,
		})
		assert.NoError(err)

		// Start RunTickerLoopNsChan
		tickerChan := make(chan *schema.Ticker, 20)
		loopCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		go mgr.RunTickerLoopNsChan(loopCtx, "ticker_ns", tickerChan, 100*time.Millisecond)

		// Collect all fires until context times out
		fireCount := 0
		done := false

		for !done {
			select {
			case receivedTicker := <-tickerChan:
				assert.NotNil(receivedTicker)
				assert.Equal("count-ticker", receivedTicker.Ticker)
				fireCount++
			case <-loopCtx.Done():
				// Context timed out, give a bit more time for any in-flight tickers
				time.Sleep(100 * time.Millisecond)
				// Drain any remaining tickers
				for len(tickerChan) > 0 {
					<-tickerChan
					fireCount++
				}
				done = true
			}
		}

		// With 250ms interval over 2000ms, we expect approximately 7-8 fires
		// (initial fire at ~250ms + ~7 more at 250ms intervals)
		// Allow some variance due to timing and test overhead
		t.Logf("Fire count: %d", fireCount)
		assert.GreaterOrEqual(fireCount, 5, "Expected at least 5 fires")
		assert.LessOrEqual(fireCount, 10, "Expected at most 10 fires")

		// Clean up
		_, err = mgr.DeleteTicker(ctx, ticker.Ticker)
		assert.NoError(err)
	})
}
