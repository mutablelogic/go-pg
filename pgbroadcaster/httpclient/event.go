package httpclient

import (
	"context"
	"fmt"

	// Packages
	client "github.com/mutablelogic/go-client"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgbroadcaster/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WatchEvents calls the function with each event which matches the filter,
// as the server streams them, until the context is cancelled, when it
// returns nil, or the function returns an error, or the stream ends. When the
// server stops, it returns ErrServiceUnavailable.
func (c *Client) WatchEvents(ctx context.Context, filter schema.EventFilter, fn func(schema.Event) error) error {
	if fn == nil {
		return pg.ErrBadParameter.With("missing function")
	}

	// Read the events, and ignore the pings. The function's error is kept,
	// as it's returned even when the context is cancelled as well.
	var fnErr, shutdown error
	callback := func(event client.TextStreamEvent) error {
		if event.Event == schema.EventShutdown {
			shutdown = pg.ErrNotAvailable.With("server is stopping")
			return nil
		} else if event.Event != schema.EventName {
			return nil
		}
		var data schema.Event
		if err := event.Json(&data); err != nil {
			return fmt.Errorf("malformed %s event: %w", event.Event, err)
		}
		fnErr = fn(data)
		return fnErr
	}
	err := c.DoWithContext(ctx, client.MethodGet, nil,
		client.OptPath("event"),
		client.OptQuery(filter.Query()),
		client.OptReqHeader("Accept", types.ContentTypeTextStream),
		client.OptTextStreamCallback(callback),
		client.OptNoTimeout(),
	)
	switch {
	case fnErr != nil:
		return fnErr
	case ctx.Err() != nil:
		return nil
	case shutdown != nil:
		return shutdown
	default:
		return err
	}
}
