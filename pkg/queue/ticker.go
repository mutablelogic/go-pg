package queue

import (
	"context"
	"errors"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - TICKER

// RegisterTicker creates a new ticker, or updates an existing ticker, and returns it.
func (manager *Manager) RegisterTicker(ctx context.Context, meta schema.TickerMeta) (*schema.Ticker, error) {
	var ticker schema.Ticker
	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		// Get a ticker
		if err := conn.Get(ctx, &ticker, schema.TickerName(meta.Ticker)); err != nil && !errors.Is(err, pg.ErrNotFound) {
			return err
		} else if errors.Is(err, pg.ErrNotFound) {
			// If the ticker does not exist, then create it
			if err := conn.Insert(ctx, &ticker, meta); err != nil {
				return err
			}
		}

		// Finally, update the ticker
		return conn.Update(ctx, &ticker, schema.TickerName(meta.Ticker), meta)
	}); err != nil {
		return nil, err
	}
	return &ticker, nil
}

// RegisterTicker creates a new ticker, or updates an existing ticker, and returns it.
func (manager *Manager) RegisterTickerNs(ctx context.Context, namespace string, meta schema.TickerMeta) (*schema.Ticker, error) {
	var ticker schema.Ticker

	// Check namespace is valid
	if !types.IsIdentifier(namespace) {
		return nil, httpresponse.ErrBadRequest.Withf("Invalid namespace %q", namespace)
	}

	// Register the ticker
	if err := manager.conn.With("ns", namespace).Tx(ctx, func(conn pg.Conn) error {
		// Get a ticker
		if err := conn.Get(ctx, &ticker, schema.TickerName(meta.Ticker)); err != nil && !errors.Is(err, pg.ErrNotFound) {
			return err
		} else if errors.Is(err, pg.ErrNotFound) {
			// If the ticker does not exist, then create it
			if err := conn.Insert(ctx, &ticker, meta); err != nil {
				return err
			}
		}

		// Finally, update the ticker
		return conn.Update(ctx, &ticker, schema.TickerName(meta.Ticker), meta)
	}); err != nil {
		return nil, err
	}
	return &ticker, nil
}

// UpdateTicker updates an existing ticker, and returns it.
func (manager *Manager) UpdateTicker(ctx context.Context, name string, meta schema.TickerMeta) (*schema.Ticker, error) {
	var ticker schema.Ticker
	if err := manager.conn.Update(ctx, &ticker, schema.TickerName(name), meta); err != nil {
		return nil, err
	}
	return &ticker, nil
}

// GetTicker returns a ticker by name
func (manager *Manager) GetTicker(ctx context.Context, name string) (*schema.Ticker, error) {
	var ticker schema.Ticker
	if err := manager.conn.Get(ctx, &ticker, schema.TickerName(name)); err != nil {
		return nil, err
	}
	return &ticker, nil
}

// DeleteTicker deletes an existing ticker, and returns the deleted ticker.
func (manager *Manager) DeleteTicker(ctx context.Context, name string) (*schema.Ticker, error) {
	var ticker schema.Ticker
	if err := manager.conn.Tx(ctx, func(conn pg.Conn) error {
		return conn.Delete(ctx, &ticker, schema.TickerName(name))
	}); err != nil {
		return nil, err
	}
	return &ticker, nil
}

// ListTickers returns all tickers in a namespace as a list
func (manager *Manager) ListTickers(ctx context.Context, req schema.TickerListRequest) (*schema.TickerList, error) {
	var list schema.TickerList
	if err := manager.conn.List(ctx, &list, req); err != nil {
		return nil, err
	}
	return &list, nil
}

// NextTickerNs returns the next matured ticker in a namespace, or nil
func (manager *Manager) NextTickerNs(ctx context.Context, namespace string) (*schema.Ticker, error) {
	var ticker schema.Ticker
	if err := manager.conn.With("ns", namespace).Get(ctx, &ticker, schema.TickerNext{}); errors.Is(err, pg.ErrNotFound) {
		// No matured ticker
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	// Return matured ticker
	return &ticker, nil
}

// NextTicker returns the next matured ticker, or nil
func (manager *Manager) NextTicker(ctx context.Context) (*schema.Ticker, error) {
	var ticker schema.Ticker
	if err := manager.conn.Get(ctx, &ticker, schema.TickerNext{}); errors.Is(err, pg.ErrNotFound) {
		// No matured ticker
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	// Return matured ticker
	return &ticker, nil
}

// RunTickerLoop runs a loop to process matured tickers in a namespace, until the context is cancelled,
// or an error occurs. The period parameter controls the initial sleep duration between checks.
func (manager *Manager) RunTickerLoop(ctx context.Context, namespace string, ch chan<- *schema.Ticker, period time.Duration) error {
	delta := period
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	prev := time.Now()

	// Loop until context is cancelled
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			// Check for matured tickers
			ticker, err := manager.NextTickerNs(ctx, namespace)
			if err != nil {
				return err
			}

			if ticker != nil {
				ch <- ticker

				// Reset timer to minimum period
				if dur := types.PtrDuration(ticker.Interval); dur >= time.Second && dur < delta {
					delta = dur
				}
				// Adjust based on time since last ticker
				if since := time.Since(prev); since > delta {
					delta += (since - delta) / 2
				} else if since < delta {
					delta -= (delta - since) / 2
				}

				// Reset the timer
				prev = time.Now()
			}

			// Next loop
			timer.Reset(delta)
		}
	}
}
