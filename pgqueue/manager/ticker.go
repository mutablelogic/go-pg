package manager

import (
	"context"
	"errors"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - TICKER

// RegisterTicker creates a new ticker, or updates an existing ticker, and returns it.
func (manager *Manager) RegisterTicker(ctx context.Context, name string, meta schema.TickerMeta, callback schema.TaskFunc) (resultPtr *schema.Ticker, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "RegisterTicker",
		attribute.String("name", name),
		attribute.String("meta", types.Stringify(meta)),
		attribute.Bool("callback", callback != nil),
	)
	defer func() { endSpan(err) }()

	var result schema.Ticker

	// Register the ticker task
	if err := manager.tickers.RegisterTask(name, callback); err != nil {
		return nil, err
	}

	// Persist the ticker
	err = manager.Tx(ctx, func(conn pg.Conn) error {
		err := conn.Get(ctx, &result, schema.TickerName(name))
		switch {
		case err == nil:
			// Ticker already exists, update below.
		case errors.Is(err, pg.ErrNotFound):
			if err := conn.With("id", name).Insert(ctx, &result, meta); err != nil {
				return err
			}
		default:
			return err
		}

		if !hasTickerMetaPatch(meta) {
			return nil
		}

		return conn.Update(ctx, &result, schema.TickerName(name), meta)
	})
	if err != nil {
		return nil, errors.Join(err, manager.tickers.RemoveTask(name))
	}

	return types.Ptr(result), nil
}

// UpdateTicker updates an existing ticker, and returns it.
func (manager *Manager) UpdateTicker(ctx context.Context, name string, meta schema.TickerMeta) (result *schema.Ticker, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "UpdateTicker",
		attribute.String("name", name),
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	var ticker schema.Ticker
	if err := manager.Update(ctx, &ticker, schema.TickerName(name), meta); err != nil {
		return nil, err
	}
	return types.Ptr(ticker), nil
}

// GetTicker returns a ticker by name.
func (manager *Manager) GetTicker(ctx context.Context, name string) (result *schema.Ticker, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "GetTicker",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	var ticker schema.Ticker
	if err := manager.Get(ctx, &ticker, schema.TickerName(name)); err != nil {
		return nil, err
	}
	return types.Ptr(ticker), nil
}

// DeleteTicker deletes an existing ticker, and returns the deleted ticker.
func (manager *Manager) DeleteTicker(ctx context.Context, name string) (result *schema.Ticker, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "DeleteTicker",
		attribute.String("name", name),
	)
	defer func() { endSpan(err) }()

	var ticker schema.Ticker
	if err := manager.Delete(ctx, &ticker, schema.TickerName(name)); err != nil {
		return nil, err
	} else if err := manager.tickers.RemoveTask(name); err != nil {
		return nil, err
	}

	// Return success
	return types.Ptr(ticker), nil
}

// ListTickers returns all tickers as a list.
func (manager *Manager) ListTickers(ctx context.Context, req schema.TickerListRequest) (result *schema.TickerList, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "ListTickers",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	resp := schema.TickerList{TickerListRequest: req}
	if err := manager.List(ctx, &resp, req); err != nil {
		return nil, err
	} else {
		resp.OffsetLimit.Clamp(resp.Count)
	}
	return types.Ptr(resp), nil
}

// NextTicker returns the next matured ticker, or nil.
func (manager *Manager) NextTicker(ctx context.Context) (result *schema.Ticker, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "NextTicker")
	defer func() { endSpan(err) }()

	var ticker schema.Ticker
	if err := manager.Get(ctx, &ticker, schema.TickerNext{}); errors.Is(err, pg.ErrNotFound) {
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
