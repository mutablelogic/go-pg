package pgqueue

import (
	"context"
	"log/slog"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
)

// RunDueTickers runs the due tickers of the manager once, as the event loop
// does when it polls them, with next in place of NextTicker, so that a test
// can change the tickers between one being found to be due and it being run
func RunDueTickers(ctx context.Context, manager *Manager, next func(context.Context) (*schema.Ticker, error)) error {
	r := &runner{
		Manager:    manager,
		logger:     slog.New(slog.DiscardHandler),
		results:    make(chan *Result, resultsBuffer),
		nextTicker: next,
	}
	return r.runDueTickers(ctx)
}
