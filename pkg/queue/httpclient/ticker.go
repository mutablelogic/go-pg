package httpclient
package httpclient

import (
	"context"
	"net/http"

	// Packages
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	client "github.com/mutablelogic/go-client"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) ListTickers(ctx context.Context, opts ...Opt) (*schema.TickerList, error) {
	req := client.NewRequest()

	// Apply options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.TickerList
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("ticker"), client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

func (c *Client) GetTicker(ctx context.Context, name string) (*schema.Ticker, error) {
	req := client.NewRequest()

	// Perform request
	var response schema.Ticker
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("ticker", name)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

func (c *Client) CreateTicker(ctx context.Context, ticker schema.TickerMeta) (*schema.Ticker, error) {
	req, err := client.NewJSONRequest(ticker)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Ticker
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("ticker")); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

func (c *Client) UpdateTicker(ctx context.Context, name string, meta schema.TickerMeta) (*schema.Ticker, error) {
	req, err := client.NewJSONRequestEx(http.MethodPatch, meta, "")
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.Ticker
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("ticker", name)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}

func (c *Client) DeleteTicker(ctx context.Context, name string) (*schema.Ticker, error) {
	req := client.NewRequestEx(http.MethodDelete, "")

	// Perform request
	var response schema.Ticker
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("ticker", name)); err != nil {
		return nil, err
	}

	// Return the responses
	return &response, nil
}
