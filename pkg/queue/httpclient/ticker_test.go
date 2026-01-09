package httpclient_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	// Packages
	httpclient "github.com/mutablelogic/go-pg/pkg/queue/httpclient"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_Client_ListTickers(t *testing.T) {
	assert := assert.New(t)

	t.Run("EmptyList", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("GET", r.Method)
			assert.Equal("/ticker", r.URL.Path)

			response := schema.TickerList{
				Count: 0,
				Body:  []schema.Ticker{},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		tickers, err := client.ListTickers(context.Background())
		assert.NoError(err)
		assert.NotNil(tickers)
		assert.Equal(uint64(0), tickers.Count)
		assert.Empty(tickers.Body)
	})

	t.Run("WithTickers", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("GET", r.Method)
			assert.Equal("/ticker", r.URL.Path)

			interval := time.Hour
			response := schema.TickerList{
				Count: 2,
				Body: []schema.Ticker{
					{
						TickerMeta: schema.TickerMeta{
							Ticker:   "ticker1",
							Interval: &interval,
						},
						Namespace: "default",
					},
					{
						TickerMeta: schema.TickerMeta{
							Ticker:   "ticker2",
							Interval: &interval,
						},
						Namespace: "default",
					},
				},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		tickers, err := client.ListTickers(context.Background())
		assert.NoError(err)
		assert.NotNil(tickers)
		assert.Equal(uint64(2), tickers.Count)
		assert.Len(tickers.Body, 2)
		assert.Equal("ticker1", tickers.Body[0].Ticker)
		assert.Equal("ticker2", tickers.Body[1].Ticker)
	})

	t.Run("WithPagination", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("GET", r.Method)
			assert.Equal("/ticker", r.URL.Path)
			assert.Equal("10", r.URL.Query().Get("offset"))
			assert.Equal("5", r.URL.Query().Get("limit"))

			response := schema.TickerList{
				Count: 0,
				Body:  []schema.Ticker{},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		limit := uint64(5)
		tickers, err := client.ListTickers(context.Background(), httpclient.WithOffsetLimit(10, &limit))
		assert.NoError(err)
		assert.NotNil(tickers)
	})

	t.Run("ServerError", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		tickers, err := client.ListTickers(context.Background())
		assert.Error(err)
		assert.Nil(tickers)
	})
}

func Test_Client_GetTicker(t *testing.T) {
	assert := assert.New(t)

	t.Run("Success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("GET", r.Method)
			assert.Equal("/ticker/myticker", r.URL.Path)

			interval := time.Hour
			response := schema.Ticker{
				TickerMeta: schema.TickerMeta{
					Ticker:   "myticker",
					Interval: &interval,
				},
				Namespace: "default",
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		ticker, err := client.GetTicker(context.Background(), "myticker")
		assert.NoError(err)
		assert.NotNil(ticker)
		assert.Equal("myticker", ticker.Ticker)
		assert.Equal("default", ticker.Namespace)
	})

	t.Run("NotFound", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		ticker, err := client.GetTicker(context.Background(), "notfound")
		assert.Error(err)
		assert.Nil(ticker)
	})
}

func Test_Client_CreateTicker(t *testing.T) {
	assert := assert.New(t)

	t.Run("Success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("POST", r.Method)
			assert.Equal("/ticker", r.URL.Path)

			var meta schema.TickerMeta
			err := json.NewDecoder(r.Body).Decode(&meta)
			assert.NoError(err)
			assert.Equal("newticker", meta.Ticker)

			interval := time.Hour
			response := schema.Ticker{
				TickerMeta: schema.TickerMeta{
					Ticker:   meta.Ticker,
					Interval: &interval,
				},
				Namespace: "default",
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		interval := time.Hour
		ticker, err := client.CreateTicker(context.Background(), schema.TickerMeta{
			Ticker:   "newticker",
			Interval: &interval,
		})
		assert.NoError(err)
		assert.NotNil(ticker)
		assert.Equal("newticker", ticker.Ticker)
	})

	t.Run("WithPayload", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("POST", r.Method)

			var meta schema.TickerMeta
			err := json.NewDecoder(r.Body).Decode(&meta)
			assert.NoError(err)
			assert.NotNil(meta.Payload)

			response := schema.Ticker{
				TickerMeta: meta,
				Namespace:  "default",
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		interval := 30 * time.Minute
		payload := json.RawMessage(`{"type":"report"}`)
		ticker, err := client.CreateTicker(context.Background(), schema.TickerMeta{
			Ticker:   "reportticker",
			Interval: &interval,
			Payload:  payload,
		})
		assert.NoError(err)
		assert.NotNil(ticker)
	})

	t.Run("Conflict", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusConflict)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		ticker, err := client.CreateTicker(context.Background(), schema.TickerMeta{
			Ticker: "existing",
		})
		assert.Error(err)
		assert.Nil(ticker)
	})
}

func Test_Client_UpdateTicker(t *testing.T) {
	assert := assert.New(t)

	t.Run("Success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("PATCH", r.Method)
			assert.Equal("/ticker/myticker", r.URL.Path)

			var meta schema.TickerMeta
			err := json.NewDecoder(r.Body).Decode(&meta)
			assert.NoError(err)
			assert.NotNil(meta.Interval)

			response := schema.Ticker{
				TickerMeta: schema.TickerMeta{
					Ticker:   "myticker",
					Interval: meta.Interval,
				},
				Namespace: "default",
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		interval := 2 * time.Hour
		ticker, err := client.UpdateTicker(context.Background(), "myticker", schema.TickerMeta{
			Interval: &interval,
		})
		assert.NoError(err)
		assert.NotNil(ticker)
		assert.Equal("myticker", ticker.Ticker)
	})

	t.Run("NotFound", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		interval := time.Hour
		ticker, err := client.UpdateTicker(context.Background(), "notfound", schema.TickerMeta{
			Interval: &interval,
		})
		assert.Error(err)
		assert.Nil(ticker)
	})
}

func Test_Client_DeleteTicker(t *testing.T) {
	assert := assert.New(t)

	t.Run("Success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal("DELETE", r.Method)
			assert.Equal("/ticker/myticker", r.URL.Path)

			interval := time.Hour
			response := schema.Ticker{
				TickerMeta: schema.TickerMeta{
					Ticker:   "myticker",
					Interval: &interval,
				},
				Namespace: "default",
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		ticker, err := client.DeleteTicker(context.Background(), "myticker")
		assert.NoError(err)
		assert.NotNil(ticker)
		assert.Equal("myticker", ticker.Ticker)
	})

	t.Run("NotFound", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client, err := httpclient.New(server.URL)
		assert.NoError(err)

		ticker, err := client.DeleteTicker(context.Background(), "notfound")
		assert.Error(err)
		assert.Nil(ticker)
	})
}
