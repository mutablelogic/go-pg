package test

import (
	"context"
	"sync"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pgmanager/manager"
	test "github.com/mutablelogic/go-pg/pkg/test"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	shared      *manager.Manager
	testCancels sync.Map // map[*testing.T]context.CancelFunc
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// Main is the test main function for auth manager tests. It starts up a container and runs the tests,
// providing a manager instance to each test.
func Main(m *testing.M, setup func(*manager.Manager) (func(), error), opts ...manager.Opt) {
	test.Main(m, func(pool pg.PoolConn) (func(), error) {
		mgr, err := manager.New(pool, opts...)
		if err != nil {
			return nil, err
		}
		shared = mgr

		teardown := func() {}
		if setup != nil {
			if teardown_, err := setup(mgr); err != nil {
				return nil, err
			} else if teardown_ != nil {
				teardown = teardown_
			}
		}

		runCtx, cancel := context.WithCancel(context.Background())
		runDone := make(chan error, 1)
		go func() {
			runDone <- mgr.Run(runCtx)
		}()
		return func() {
			cancelAllTestContexts()
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
		t.Fatal("test manager is not initialized; call auth/test.Main from TestMain")
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
	testCancels.Store(t, context.CancelFunc(stop))
	t.Cleanup(func() {
		if cancelAny, ok := testCancels.LoadAndDelete(t); ok {
			if cancel, ok := cancelAny.(context.CancelFunc); ok {
				cancel()
			}
		}
	})
	return shared, ctx
}

// End releases the per-test context created by Begin.
func End(t *testing.T) {
	t.Helper()
	if cancelAny, ok := testCancels.LoadAndDelete(t); ok {
		if cancel, ok := cancelAny.(context.CancelFunc); ok {
			cancel()
		}
	}
}

func cancelAllTestContexts() {
	testCancels.Range(func(key, value any) bool {
		if cancel, ok := value.(context.CancelFunc); ok {
			cancel()
		}
		testCancels.Delete(key)
		return true
	})
}
