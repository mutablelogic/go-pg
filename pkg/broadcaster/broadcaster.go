package broadcaster

import (
	"context"
	"encoding/json"
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

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

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

func (b *broadcaster) Close() {
	b.cancel()
	b.wg.Wait()
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (b *broadcaster) Subscribe(ctx context.Context, callback func(ChangeNotification)) error {
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

func (b *broadcaster) broadcast(change ChangeNotification) {
	b.mu.RLock()
	subscribers := slices.Clone(b.subscribers)
	b.mu.RUnlock()

	for _, subscriber := range subscribers {
		// Skip subscribers that were removed after the snapshot was taken.
		select {
		case <-subscriber.done:
			continue
		default:
		}

		// Deliver the change unless the subscriber or broadcaster has been canceled.
		select {
		case subscriber.ch <- change:
		case <-subscriber.done:
		case <-b.ctx.Done():
			return
		}
	}
}

func (b *broadcaster) unsubscribe(subscriber *subscription) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Remove the subscriber and signal any in-flight broadcasts to skip it.
	if i := slices.Index(b.subscribers, subscriber); i >= 0 {
		close(subscriber.done)
		b.subscribers = slices.Delete(b.subscribers, i, i+1)
	}
}
