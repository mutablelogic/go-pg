package broadcaster

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgbroadcaster/schema"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// reconnectDelay is how long Run waits to listen again after it can't,
	// which doubles each time it can't, up to reconnectMaxDelay
	reconnectDelay    = 250 * time.Millisecond
	reconnectMaxDelay = 30 * time.Second
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Run listens for the notifications on the manager's channel, and publishes
// each, as a change, to the broadcaster, logging with the logger, until the
// context is cancelled, and runs the broadcaster, when it's the manager's,
// so its watches end when it stops. A notification which isn't a change,
// such as one whose action isn't INSERT, UPDATE or DELETE, is logged, and
// skipped. When the connection which listens closes, such as when it fails,
// it listens again, and the notifications in between are missed. When it
// can't listen again, such as while the database restarts, it logs that,
// and tries again, after a delay which doubles each time, until it can, or
// the context is cancelled. When it can't listen at first, it returns the
// error. Only one can run at a time.
func (self *Manager) Run(ctx context.Context, logger *slog.Logger) error {
	if !self.running.CompareAndSwap(false, true) {
		return pg.ErrConflict.With("broadcaster is already running")
	}
	defer self.running.Store(false)
	if logger == nil {
		logger = slog.Default()
	}

	// Run the manager's broadcaster, until it stops, so its watches end
	if !self.shared {
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = self.broadcaster.Run(runCtx)
		}()
		defer func() {
			cancel()
			<-done
		}()
	}

	listened, delay := false, reconnectDelay
	for ctx.Err() == nil {
		// Listen on the channel. Being cancelled while it does is stopping.
		notifications, err := self.PoolConn.Subscribe(ctx, self.channel)
		if err != nil {
			switch {
			case ctx.Err() != nil:
				return nil
			case !listened:
				return err
			}

			// Try to listen again, after the delay, which doubles each time
			logger.WarnContext(ctx, "can't listen for changes, trying again", "channel", self.channel, "delay", delay.String(), "error", err.Error())
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(delay):
			}
			delay = min(delay*2, reconnectMaxDelay)
			continue
		}
		listened, delay = true, reconnectDelay
		self.setListening(true)
		logger.DebugContext(ctx, "listening for changes", "channel", self.channel)

		// Publish each notification to the broadcaster, until the connection
		// closes
		for notification := range notifications {
			var n schema.Notification
			if err := json.Unmarshal(notification.Payload, &n); err != nil {
				logger.WarnContext(ctx, "not a change", "channel", notification.Channel, "payload", string(notification.Payload), "error", err.Error())
				continue
			}
			change, err := n.Change()
			if err != nil {
				logger.WarnContext(ctx, "not a change", "channel", notification.Channel, "payload", string(notification.Payload), "error", err.Error())
				continue
			}
			logger.DebugContext(ctx, "change", "channel", notification.Channel, "payload", string(notification.Payload))
			if missed := self.broadcaster.Publish(change); missed > 0 {
				logger.DebugContext(ctx, "watchers missed a change, as they weren't ready for it", "channel", notification.Channel, "watchers", missed)
			}
		}
		self.setListening(false)
		if ctx.Err() == nil {
			logger.WarnContext(ctx, "stopped listening for changes, listening again", "channel", self.channel)
		}
	}
	return nil
}

// Running reports whether the manager is running
func (self *Manager) Running() bool {
	return self.running.Load()
}

// Listening reports whether the manager is listening on its channel, so the
// changes published to it are published to the broadcaster
func (self *Manager) Listening() bool {
	return self.listening.Load()
}

// WaitListening waits until the manager is listening on its channel, so the
// changes made from then are published to the broadcaster, or returns the
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
