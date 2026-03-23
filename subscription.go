package pg

import (
	"context"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type subscription struct {
	cancel context.CancelFunc
	done   chan struct{}
}

type subscriptionArray struct {
	mu     sync.Mutex
	closed bool
	items  []*subscription
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func newSubscriptionArray() *subscriptionArray {
	return &subscriptionArray{}
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (a *subscriptionArray) Add(cancel context.CancelFunc) (*subscription, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil, ErrNotAvailable.With("subscriptions are closed")
	}

	sub := &subscription{cancel: cancel, done: make(chan struct{})}
	a.items = append(a.items, sub)
	return sub, nil
}

func (a *subscriptionArray) Remove(sub *subscription) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for i, item := range a.items {
		if item != sub {
			continue
		}
		a.items = append(a.items[:i], a.items[i+1:]...)
		return
	}
}

func (a *subscriptionArray) Close() []*subscription {
	a.mu.Lock()
	if a.closed {
		items := append([]*subscription(nil), a.items...)
		a.mu.Unlock()
		return items
	}
	a.closed = true
	items := append([]*subscription(nil), a.items...)
	a.mu.Unlock()

	for _, sub := range items {
		sub.cancel()
	}

	return items
}

func (s *subscription) Done() {
	close(s.done)
}

func (s *subscription) Wait() {
	<-s.done
}
