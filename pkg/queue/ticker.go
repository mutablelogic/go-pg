package queue

import (
	"context"
	"errors"
	"time"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
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

// runTickerLoopChan runs a loop to process matured tickers, sending them to a channel.
// This is the internal implementation used by RunTickerLoop.
func (manager *Manager) runTickerLoopChan(ctx context.Context, ch chan<- *schema.Ticker, period time.Duration) error {
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			// Create a span for the ticker polling operation to nest NextTicker database calls
			pollCtx, endspan := otel.StartSpan(manager.tracer, ctx, spanManagerName("ticker.poll"))
			ticker, err := manager.NextTicker(pollCtx)
			endspan(err)
			if err != nil {
				return err
			}
			if ticker != nil {
				ch <- ticker
				timer.Reset(time.Millisecond)
			} else {
				timer.Reset(period)
			}
		}
	}
}

// runTickerLoopNsChan runs a loop to process matured tickers in a namespace, sending them to a channel.
// This is the internal implementation used by RunTickerLoopNs.
func (manager *Manager) runTickerLoopNsChan(ctx context.Context, namespace string, ch chan<- *schema.Ticker, period time.Duration) error {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	// Loop until context is cancelled
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			// Create a span for the ticker polling operation to nest NextTickerNs database calls
			pollCtx, endspan := otel.StartSpan(manager.tracer, ctx, spanManagerName("ticker.poll"))
			// Check for matured tickers
			ticker, err := manager.NextTickerNs(pollCtx, namespace)
			endspan(err)
			if err != nil {
				return err
			}

			if ticker != nil {
				ch <- ticker
				// Ticker found - poll again immediately to drain any other matured tickers
				timer.Reset(1 * time.Millisecond)
			} else {
				// No ticker found - wait for the full period
				timer.Reset(period)
			}
		}
	}
}

// RunTickerLoopChan runs a loop to process matured tickers, sending them to a channel.
// Use this for streaming scenarios where you need direct channel access.
// The caller owns the channel and must close it when done.
// Blocks until context is cancelled or an error occurs.
func (manager *Manager) RunTickerLoopChan(ctx context.Context, ch chan<- *schema.Ticker, period time.Duration) error {
	return manager.runTickerLoopChan(ctx, ch, period)
}

// RunTickerLoopNsChan runs a loop to process matured tickers in a namespace, sending them to a channel.
// Use this for streaming scenarios where you need direct channel access.
// The caller owns the channel and must close it when done.
// Blocks until context is cancelled or an error occurs.
func (manager *Manager) RunTickerLoopNsChan(ctx context.Context, namespace string, ch chan<- *schema.Ticker, period time.Duration) error {
	return manager.runTickerLoopNsChan(ctx, namespace, ch, period)
}

// RunTickerLoop runs a loop to process matured tickers with a single worker.
// The handler is called for each matured ticker.
// Blocks until context is cancelled or an error occurs.
func (manager *Manager) RunTickerLoop(ctx context.Context, period time.Duration, handler TickerHandler) error {
	// Unbuffered channel - blocks send until handler is ready
	tickers := make(chan *schema.Ticker)

	// Error channel for fatal errors
	errCh := make(chan error, 1)

	// Run the ticker loop in a goroutine
	go func() {
		defer close(tickers)
		if err := manager.runTickerLoopChan(ctx, tickers, period); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}()

	// Process tickers
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			return err
		case ticker, ok := <-tickers:
			if !ok {
				return nil
			}
			if err := handler(ctx, ticker); err != nil {
				// Handler errors don't stop the loop (errors are recorded in spans)
				// Example: no worker registered for ticker, worker execution failures
				_ = err
			}
		}
	}
}

// RunTickerLoopNs runs a loop to process matured tickers in a specific namespace with a single worker.
// The handler is called for each matured ticker.
// Blocks until context is cancelled or an error occurs.
func (manager *Manager) RunTickerLoopNs(ctx context.Context, namespace string, period time.Duration, handler TickerHandler) error {
	// Unbuffered channel - blocks send until handler is ready
	tickers := make(chan *schema.Ticker)

	// Error channel for fatal errors
	errCh := make(chan error, 1)

	// Run the ticker loop in a goroutine
	go func() {
		defer close(tickers)
		if err := manager.runTickerLoopNsChan(ctx, namespace, tickers, period); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}()

	// Process tickers
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			return err
		case ticker, ok := <-tickers:
			if !ok {
				return nil
			}
			if err := handler(ctx, ticker); err != nil {
				// Handler errors don't stop the loop (errors are recorded in spans)
				// Example: no worker registered for ticker, worker execution failures
				_ = err
			}
		}
	}
}
