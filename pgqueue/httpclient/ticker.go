package httpclient

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListTickers returns a page of tickers in name order, with the total number of
// tickers
func (c *Client) ListTickers(ctx context.Context, req schema.TickerListRequest) (*schema.TickerList, error) {
	var response schema.TickerList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("ticker"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}

// GetTicker returns a ticker by name
func (c *Client) GetTicker(ctx context.Context, name string) (*schema.Ticker, error) {
	var response schema.Ticker
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("ticker", name)); err != nil {
		return nil, err
	}

	// Return the response
	return types.Ptr(response), nil
}
