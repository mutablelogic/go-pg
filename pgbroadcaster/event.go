package broadcaster

import (
	"context"
	"encoding/json"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgbroadcaster/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// errStopped ends a subscription, as the manager has stopped running
var errStopped = pg.ErrNotAvailable.With("broadcaster stopped")

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Subscribe sends the events published to the manager's channel which match
// the filter to ch, from when it returns, as it's subscribed before it does,
// until the context is cancelled, or the manager stops running, as there will
// be no more events. It returns a channel which is sent nil when the context
// is cancelled, or ErrServiceUnavailable when the manager stops, or has
// stopped, once the subscription has ended, after which no more events are
// sent to ch, so it can be closed. A subscription made before the manager
// first runs lasts until it stops, and one made once it's stopped isn't
// made, and ErrServiceUnavailable is returned, so the server, as it stops,
// doesn't accept them. The channel ch isn't closed. An event
// isn't waited for a subscriber to receive, so a subscriber whose channel is
// full misses it, which coalesces events into the one in the channel, or
// which, when it needs every event, should buffer its channel.
func (self *Manager) Subscribe(ctx context.Context, filter schema.EventFilter, ch chan<- schema.Event) (<-chan error, error) {
	if ch == nil {
		return nil, pg.ErrBadParameter.With("missing channel")
	}
	done := make(chan error, 1)

	// A manager which has stopped has no more events, so it isn't
	// subscribed to
	sub := &subscriber{filter: filter, stop: make(chan struct{})}
	self.mu.Lock()
	if self.stopped {
		self.mu.Unlock()
		return nil, errStopped
	}
	self.subscribers[ch] = sub
	self.mu.Unlock()

	// The subscription ends when the context is cancelled, or the manager
	// stops. It's removed before that's sent, under the lock events are
	// sent under, so once it's received, no more events are sent to ch, and
	// it can be closed.
	go func() {
		var err error
		select {
		case <-ctx.Done():
		case <-sub.stop:
			err = errStopped
		}
		self.mu.Lock()
		if self.subscribers[ch] == sub {
			delete(self.subscribers, ch)
		}
		self.mu.Unlock()
		done <- err
	}()
	return done, nil
}

// Publish publishes the event to the manager's channel, as JSON, so every
// server listening on it is told of it. Returns ErrBadParameter when the
// event is larger than a notification can be.
func (self *Manager) Publish(ctx context.Context, event schema.Event) (err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgbroadcaster.Publish",
		attribute.String("arg.channel", self.channel),
		attribute.String("arg.event", types.Stringify(event)),
	)
	defer func() { endSpan(err) }()

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	} else if len(payload) > schema.MaxPayloadSize {
		return pg.ErrBadParameter.Withf("event is %d bytes, which is more than %d", len(payload), schema.MaxPayloadSize)
	}
	return self.PoolConn.With("channel", self.channel, "payload", string(payload)).Exec(ctx, "${pgbroadcaster.publish}")
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// broadcast sends the event to every subscriber whose filter it matches,
// which can receive it now, and returns how many couldn't, as their channels
// are full
func (self *Manager) broadcast(event schema.Event) int {
	self.mu.Lock()
	defer self.mu.Unlock()
	var missed int
	for ch, sub := range self.subscribers {
		if !sub.filter.Matches(event) {
			continue
		}
		select {
		case ch <- event:
		default:
			missed++
		}
	}
	return missed
}

// unsubscribeAll ends every subscription, as the manager has stopped running,
// and those made later, until it runs again
func (self *Manager) unsubscribeAll() {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.stopped = true
	for ch, sub := range self.subscribers {
		close(sub.stop)
		delete(self.subscribers, ch)
	}
}
