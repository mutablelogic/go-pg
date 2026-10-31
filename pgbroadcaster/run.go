package broadcaster

import (
	"context"
	"encoding/json"
	"log/slog"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgbroadcaster/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Run listens for the notifications on the manager's channel, and sends
// each, as an event, to the subscribers, logging with the logger, until the
// context is cancelled. A notification which isn't an event is logged, and
// skipped. When the connection which
// listens closes, such as when it fails, it listens again, and the
// notifications in between are missed. When it stops, every subscription
// ends. Only one can run at a time.
func (self *Manager) Run(ctx context.Context, logger *slog.Logger) error {
	if !self.running.CompareAndSwap(false, true) {
		return pg.ErrConflict.With("broadcaster is already running")
	}
	defer self.running.Store(false)
	defer self.unsubscribeAll()
	self.mu.Lock()
	self.stopped = false
	self.mu.Unlock()
	if logger == nil {
		logger = slog.Default()
	}

	for ctx.Err() == nil {
		// Listen on the channel. Being cancelled while it does is stopping.
		notifications, err := self.PoolConn.Subscribe(ctx, self.channel)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		self.setListening(true)
		logger.DebugContext(ctx, "listening for events", "channel", self.channel)

		// Send each notification to the subscribers, until the connection
		// closes
		for notification := range notifications {
			var event schema.Event
			if err := json.Unmarshal(notification.Payload, &event); err != nil {
				logger.WarnContext(ctx, "not an event", "channel", notification.Channel, "payload", string(notification.Payload), "error", err.Error())
				continue
			}
			logger.DebugContext(ctx, "event", "channel", notification.Channel, "payload", string(notification.Payload))
			if missed := self.broadcast(event); missed > 0 {
				logger.DebugContext(ctx, "subscribers missed an event, as they weren't ready for it", "channel", notification.Channel, "subscribers", missed)
			}
		}
		self.setListening(false)
		if ctx.Err() == nil {
			logger.WarnContext(ctx, "stopped listening for events, listening again", "channel", self.channel)
		}
	}
	return nil
}

// Running reports whether the manager is running
func (self *Manager) Running() bool {
	return self.running.Load()
}

// Listening reports whether the manager is listening on its channel, so the
// events published to it are sent to the subscribers
func (self *Manager) Listening() bool {
	return self.listening.Load()
}

// WaitListening waits until the manager is listening on its channel, so the
// changes made from then are sent to the subscribers, or returns the
// context's error when it's cancelled first
func (self *Manager) WaitListening(ctx context.Context) error {
	self.mu.Lock()
	ready := self.ready
	self.mu.Unlock()
	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Subscribers returns how many subscriptions there are
func (self *Manager) Subscribers() int {
	self.mu.Lock()
	defer self.mu.Unlock()
	return len(self.subscribers)
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// setListening sets whether the manager is listening on its channel, and
// closes the channel WaitListening waits on once it is, or replaces it once
// it isn't
func (self *Manager) setListening(listening bool) {
	self.mu.Lock()
	defer self.mu.Unlock()
	switch {
	case listening && !self.listening.Load():
		close(self.ready)
	case !listening && self.listening.Load():
		self.ready = make(chan struct{})
	}
	self.listening.Store(listening)
}
