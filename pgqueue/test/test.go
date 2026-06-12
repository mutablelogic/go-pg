package test

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pgqueue/manager"
	test "github.com/mutablelogic/go-pg/pkg/test"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	shared  *manager.Manager
	cancels cancelRegistry
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type cancelRegistry struct {
	mu      sync.Mutex
	cancels map[*testing.T]context.CancelFunc
}

func (r *cancelRegistry) Store(t *testing.T, cancel context.CancelFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancels == nil {
		r.cancels = make(map[*testing.T]context.CancelFunc)
	}
	r.cancels[t] = cancel
}

func (r *cancelRegistry) LoadAndDelete(t *testing.T) (context.CancelFunc, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cancels == nil {
		return nil, false
	}
	cancel, ok := r.cancels[t]
	if ok {
		delete(r.cancels, t)
	}
	return cancel, ok
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// Main is the test main function for tests. It starts up a container and runs the tests,
// providing a manager instance to each test.
func Main(m *testing.M, setup func(*manager.Manager) (func(), error), opts ...manager.Opt) {
	test.Main(m, func(pool pg.PoolConn) (func(), error) {
		manager, err := manager.New(context.Background(), pool, opts...)
		if err != nil {
			return nil, err
		}
		shared = manager

		teardown := func() {}
		if setup != nil {
			if teardown_, err := setup(manager); err != nil {
				return nil, err
			} else if teardown_ != nil {
				teardown = teardown_
			}
		}

		runCtx, cancel := context.WithCancel(context.Background())
		runDone := make(chan error, 1)
		go func() {
			runDone <- manager.Run(runCtx, slog.Default())
		}()
		return func() {
			cancel()
			if err := <-runDone; err != nil {
				panic(err)
			}
			shared = nil
			teardown()
		}, nil
	})
}

// Begin returns the shared test manager and a per-test context.
func Begin(t *testing.T) (*manager.Manager, context.Context) {
	t.Helper()
	if shared == nil {
		t.Fatal("test manager is not initialized; call queue/test.Main from TestMain")
	}
	base := context.Background()
	baseCancel := func() {}
	if deadline, ok := t.Deadline(); ok {
		base, baseCancel = context.WithDeadline(base, deadline)
	}
	ctx, cancel := context.WithCancel(base)
	stop := func() {
		cancel()
		baseCancel()
	}
	cancels.Store(t, context.CancelFunc(stop))
	t.Cleanup(func() {
		if cancel, ok := cancels.LoadAndDelete(t); ok {
			cancel()
		}
	})
	return shared, ctx
}

// End releases the per-test context created by Begin.
func End(t *testing.T) {
	t.Helper()
	if cancel, ok := cancels.LoadAndDelete(t); ok {
		cancel()
	}
}
