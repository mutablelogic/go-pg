package httphandler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	// Packages
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	httphandler "github.com/mutablelogic/go-pg/pkg/queue/httphandler"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	test "github.com/mutablelogic/go-pg/pkg/test"
	assert "github.com/stretchr/testify/assert"
)

func Test_Ticker_List(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	container, pool, err := test.NewPgxContainer(ctx, "ticker_list_test", testing.Verbose(), nil)
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	mgr, err := queue.New(ctx, pool, "test_list")
	assert.NoError(err)

	// Create test tickers
	for i := 1; i <= 5; i++ {
		tickerName := fmt.Sprintf("test_ticker_%d", i)
		interval := time.Duration(i) * time.Minute
		_, err := mgr.RegisterTicker(ctx, schema.TickerMeta{Ticker: tickerName, Interval: &interval})
		assert.NoError(err)
		defer mgr.DeleteTicker(ctx, tickerName)
	}

	router := http.NewServeMux()
	httphandler.RegisterTickerHandlers(router, "/api", mgr, nil)

	t.Run("ListTickers", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/ticker", nil)
		// Test without Accept header - should default to application/json
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if !assert.Equal(http.StatusOK, w.Code) {
			t.Logf("Response: %s", w.Body.String())
		}
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.TickerList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.GreaterOrEqual(int(resp.Count), 5)
	})

	t.Run("ListTickersWithAcceptHeader", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/ticker", nil)
		req.Header.Set("Accept", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.TickerList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.GreaterOrEqual(int(resp.Count), 5)
	})

	t.Run("ListTickersWithLimit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/ticker?limit=2", nil)
		req.Header.Set("Accept", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.TickerList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal(2, len(resp.Body))
	})

	t.Run("ListTickersWithOffset", func(t *testing.T) {
		// Get all tickers first
		req := httptest.NewRequest(http.MethodGet, "/api/ticker", nil)
		req.Header.Set("Accept", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		var allResp schema.TickerList
		err := json.Unmarshal(w.Body.Bytes(), &allResp)
		assert.NoError(err)

		// Get with offset=2
		req = httptest.NewRequest(http.MethodGet, "/api/ticker?offset=2", nil)
		req.Header.Set("Accept", "application/json")
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		var resp schema.TickerList
		err = json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal(len(allResp.Body)-2, len(resp.Body))
	})

	t.Run("ListTickersWithLimitAndOffset", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/ticker?limit=2&offset=1", nil)
		req.Header.Set("Accept", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var resp schema.TickerList
		err := json.Unmarshal(w.Body.Bytes(), &resp)
		assert.NoError(err)
		assert.Equal(2, len(resp.Body))
	})
}

func Test_Ticker_Create(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	container, pool, err := test.NewPgxContainer(ctx, "ticker_create_test", testing.Verbose(), nil)
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	mgr, err := queue.New(ctx, pool, "test_create")
	assert.NoError(err)

	router := http.NewServeMux()
	httphandler.RegisterTickerHandlers(router, "/api", mgr, nil)

	t.Run("CreateSuccess", func(t *testing.T) {
		body := `{"ticker": "test_http_create", "duration": 60000000000}`
		req := httptest.NewRequest(http.MethodPost, "/api/ticker", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if !assert.Equal(http.StatusCreated, w.Code) {
			t.Logf("Response: %s", w.Body.String())
		}

		defer mgr.DeleteTicker(ctx, "test_http_create")
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		body := `{"ticker": "test_duplicate", "duration": 60000000000}`
		req := httptest.NewRequest(http.MethodPost, "/api/ticker", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)
		if !assert.Equal(http.StatusCreated, w.Code) {
			t.Logf("Response: %s", w.Body.String())
		}

		req = httptest.NewRequest(http.MethodPost, "/api/ticker", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w = httptest.NewRecorder()

		router.ServeHTTP(w, req)
		if !assert.Equal(http.StatusConflict, w.Code) {
			t.Logf("Response: %s", w.Body.String())
		}

		defer mgr.DeleteTicker(ctx, "test_duplicate")
	})
}

func Test_Ticker_Get(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	container, pool, err := test.NewPgxContainer(ctx, "ticker_get_test", testing.Verbose(), nil)
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	mgr, err := queue.New(ctx, pool, "test_get")
	assert.NoError(err)

	router := http.NewServeMux()
	httphandler.RegisterTickerHandlers(router, "/api", mgr, nil)

	t.Run("GetExisting", func(t *testing.T) {
		// Create a ticker first
		body := `{"ticker": "test_get_ticker", "duration": 60000000000}`
		req := httptest.NewRequest(http.MethodPost, "/api/ticker", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(http.StatusCreated, w.Code)
		defer mgr.DeleteTicker(ctx, "test_get_ticker")

		// Get the ticker
		req = httptest.NewRequest(http.MethodGet, "/api/ticker/test_get_ticker", nil)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var ticker schema.Ticker
		err := json.Unmarshal(w.Body.Bytes(), &ticker)
		assert.NoError(err)
		assert.Equal("test_get_ticker", ticker.Ticker)
	})

	t.Run("GetNotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/ticker/nonexistent", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})
}

func Test_Ticker_Update(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	container, pool, err := test.NewPgxContainer(ctx, "ticker_update_test", testing.Verbose(), nil)
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	mgr, err := queue.New(ctx, pool, "test_update")
	assert.NoError(err)

	router := http.NewServeMux()
	httphandler.RegisterTickerHandlers(router, "/api", mgr, nil)

	t.Run("UpdateSuccess", func(t *testing.T) {
		// Create a ticker first
		body := `{"ticker": "test_update_ticker", "duration": 60000000000}`
		req := httptest.NewRequest(http.MethodPost, "/api/ticker", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(http.StatusCreated, w.Code)
		defer mgr.DeleteTicker(ctx, "test_update_ticker")

		// Update the ticker
		updateBody := `{"ticker": "test_update_ticker", "duration": 120000000000}`
		req = httptest.NewRequest(http.MethodPatch, "/api/ticker/test_update_ticker", bytes.NewBufferString(updateBody))
		req.Header.Set("Content-Type", "application/json")
		w = httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if !assert.Equal(http.StatusOK, w.Code) {
			t.Logf("Response: %s", w.Body.String())
		}
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var ticker schema.Ticker
		err := json.Unmarshal(w.Body.Bytes(), &ticker)
		assert.NoError(err)
		assert.Equal("test_update_ticker", ticker.Ticker)
	})

	t.Run("UpdateNotFound", func(t *testing.T) {
		body := `{"ticker": "nonexistent", "duration": 120000000000}`
		req := httptest.NewRequest(http.MethodPatch, "/api/ticker/nonexistent", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})
}

func Test_Ticker_Delete(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	container, pool, err := test.NewPgxContainer(ctx, "ticker_delete_test", testing.Verbose(), nil)
	assert.NoError(err)
	defer pool.Close()
	defer container.Close(ctx)

	mgr, err := queue.New(ctx, pool, "test_delete")
	assert.NoError(err)

	router := http.NewServeMux()
	httphandler.RegisterTickerHandlers(router, "/api", mgr, nil)

	t.Run("DeleteSuccess", func(t *testing.T) {
		// Create a ticker first
		body := `{"ticker": "test_delete_ticker", "duration": 60000000000}`
		req := httptest.NewRequest(http.MethodPost, "/api/ticker", bytes.NewBufferString(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(http.StatusCreated, w.Code)

		// Delete the ticker
		req = httptest.NewRequest(http.MethodDelete, "/api/ticker/test_delete_ticker", nil)
		w = httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusOK, w.Code)
		assert.Contains(w.Header().Get("Content-Type"), "application/json")

		var ticker schema.Ticker
		err := json.Unmarshal(w.Body.Bytes(), &ticker)
		assert.NoError(err)
		assert.Equal("test_delete_ticker", ticker.Ticker)

		// Verify ticker is deleted
		req = httptest.NewRequest(http.MethodGet, "/api/ticker/test_delete_ticker", nil)
		w = httptest.NewRecorder()
		router.ServeHTTP(w, req)
		assert.Equal(http.StatusNotFound, w.Code)
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/ticker/nonexistent", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		assert.Equal(http.StatusNotFound, w.Code)
	})
}
