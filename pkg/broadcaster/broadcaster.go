package broadcaster

import (
	"context"
	"encoding/json"
	"io"
	"slices"
	"sync"

	// Packages
	pg "github.com/mutablelogic/go-pg"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Broadcaster delivers decoded change notifications to callbacks until the
// subscriber context or broadcaster lifetime ends.
type Broadcaster interface {
	io.Closer
	Subscribe(context.Context, func(ChangeNotification)) error
}

type subscription struct {
	ch   chan ChangeNotification
	done chan struct{}
}

type broadcaster struct {
	mu          sync.RWMutex
	subscribers []*subscription
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

var _ Broadcaster = (*broadcaster)(nil)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewBroadcaster subscribes to a PostgreSQL channel
func NewBroadcaster(conn pg.Conn, channel string) (*broadcaster, error) {
	self := new(broadcaster)

	// Set up the context and done channel
	ctx, cancel := context.WithCancel(context.Background())
	self.ctx = ctx
	self.cancel = cancel

	// Subscribe to the PostgreSQL channel
	notifications, err := conn.Subscribe(ctx, channel)
	if err != nil {
		cancel()
		return nil, err
	}

	// Respond to notifications in a separate goroutine
	self.wg.Add(1)
	go func() {
		defer self.wg.Done()
		for notification := range notifications {
			var change ChangeNotification
			// Skip any bad JSON
			if err := json.Unmarshal(notification.Payload, &change); err != nil {
				continue
			}
			self.broadcast(change)
		}
	}()

	// Return success
	return self, nil
}

// Close stops the broadcaster and waits for its worker goroutines to exit.
func (b *broadcaster) Close() error {
	b.cancel()
	b.wg.Wait()
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Subscribe registers a callback that receives decoded change notifications
// until the subscriber context or broadcaster is closed.
func (b *broadcaster) Subscribe(ctx context.Context, callback func(ChangeNotification)) error {
	if callback == nil {
		return pg.ErrBadParameter.With("callback is required")
	}

	// Create a new channel for this subscriber
	subscriber := &subscription{
		ch:   make(chan ChangeNotification, 10), // Buffered channel to avoid blocking
		done: make(chan struct{}),
	}

	b.mu.Lock()
	b.subscribers = append(b.subscribers, subscriber)
	b.mu.Unlock()

	// Start a goroutine to listen for notifications and call the callback
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		defer b.unsubscribe(subscriber)
		for {
			select {
			case change := <-subscriber.ch:
				callback(change)
			case <-ctx.Done():
				return
			case <-b.ctx.Done():
				return
			}
		}
	}()

	return nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// broadcast delivers a change notification to all active subscribers.
func (b *broadcaster) broadcast(change ChangeNotification) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// Deliver the change unless the broadcaster has been canceled.
	for _, subscriber := range b.subscribers {
		select {
		case subscriber.ch <- change:
		case <-b.ctx.Done():
			return
		}
	}
}

// unsubscribe removes a subscriber from the active broadcast set.
func (b *broadcaster) unsubscribe(subscriber *subscription) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Remove the subscriber and signal any in-flight broadcasts to skip it.
	if i := slices.Index(b.subscribers, subscriber); i >= 0 {
		close(subscriber.done)
		b.subscribers = slices.Delete(b.subscribers, i, i+1)
	}
}
