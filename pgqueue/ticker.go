package pgqueue

import (
	"context"
	"errors"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - TICKER

// RegisterTicker creates a new ticker, or updates an existing ticker, and returns it.
func (self *Manager) RegisterTicker(ctx context.Context, name string, meta schema.TickerMeta, callback schema.TaskFunc) (resultPtr *schema.Ticker, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.RegisterTicker",
		attribute.String("name", name),
		attribute.String("meta", types.Stringify(meta)),
		attribute.Bool("callback", callback != nil),
	)
	defer func() { endSpan(err) }()

	var result schema.Ticker

	// Register the ticker task
	if err := self.tickers.RegisterTask(name, callback); err != nil {
		return nil, err
	}

	// Persist the ticker
	err = self.Tx(ctx, func(conn pg.Conn) error {
		// Insert the ticker, unless it exists, such as when another process
		// sharing the database registers it at the same time, and then get it
		if err := conn.With("id", name).Insert(ctx, &result, meta); errors.Is(err, pg.ErrNotFound) {
			if err := conn.Get(ctx, &result, schema.TickerName(name)); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}

		if !hasTickerMetaPatch(meta) {
			return nil
		}

		return conn.Update(ctx, &result, schema.TickerName(name), meta)
	})
	if err != nil {
		return nil, errors.Join(err, self.tickers.RemoveTask(name))
	}

	return types.Ptr(result), nil
}

// UpdateTicker updates an existing ticker, and returns it.
func (self *Manager) UpdateTicker(ctx context.Context, name string, meta schema.TickerMeta) (result *schema.Ticker, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.UpdateTicker",
		attribute.String("name", name),
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	var ticker schema.Ticker
	if err := self.Update(ctx, &ticker, schema.TickerName(name), meta); err != nil {
		return nil, err
	}
	return types.Ptr(ticker), nil
}

// GetTicker returns a ticker by name.
func (self *Manager) GetTicker(ctx context.Context, name string) (result *schema.Ticker, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.GetTicker",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	var ticker schema.Ticker
	if err := self.Get(ctx, &ticker, schema.TickerName(name)); err != nil {
		return nil, err
	}
	return types.Ptr(ticker), nil
}

// DeleteTicker deletes an existing ticker, and returns the deleted ticker.
func (self *Manager) DeleteTicker(ctx context.Context, name string) (result *schema.Ticker, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.DeleteTicker",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	// Delete the ticker, and its task callback. A ticker registered by another
	// process sharing the database has no callback in this one.
	var ticker schema.Ticker
	if err := self.Delete(ctx, &ticker, schema.TickerName(name)); err != nil {
		return nil, err
	} else if err := self.tickers.RemoveTask(name); err != nil && !errors.Is(err, httpresponse.ErrNotFound) {
		return nil, err
	}

	// Return success
	return types.Ptr(ticker), nil
}

// ListTickers returns all tickers as a list.
func (self *Manager) ListTickers(ctx context.Context, req schema.TickerListRequest) (result *schema.TickerList, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.ListTickers",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	resp := schema.TickerList{TickerListRequest: req}
	if err := self.List(ctx, &resp, req); err != nil {
		return nil, err
	} else {
		resp.OffsetLimit.Clamp(resp.Count)
	}
	return types.Ptr(resp), nil
}

// NextTicker returns the next due ticker of the tickers with callbacks in
// this process, and marks it as run now, or returns nil if none is due. The
// tickers of another process sharing the database aren't returned.
func (self *Manager) NextTicker(ctx context.Context) (result *schema.Ticker, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.NextTicker")
	defer func() { endSpan(err) }()

	// No ticker is due without callbacks
	names := self.tickers.Names()
	if len(names) == 0 {
		return nil, nil
	}

	var ticker schema.Ticker
	if err := self.Get(ctx, &ticker, schema.TickerNext{Tickers: names}); errors.Is(err, pg.ErrNotFound) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	return types.Ptr(ticker), nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func hasTickerMetaPatch(meta schema.TickerMeta) bool {
	return meta.Interval != nil || len(meta.Payload) > 0
}
