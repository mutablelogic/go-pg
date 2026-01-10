package queue_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	// Packages
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	assert "github.com/stretchr/testify/assert"
)

////////////////////////////////////////////////////////////////////////////////
// WORKER POOL TESTS

func Test_WorkerPool_New(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_wp"))
	assert.NoError(err)

	t.Run("DefaultOptions", func(t *testing.T) {
		pool, err := queue.NewWorkerPool(mgr)
		assert.NoError(err)
		assert.NotNil(pool)
	})

	t.Run("WithWorkers", func(t *testing.T) {
		pool, err := queue.NewWorkerPool(mgr, queue.WithWorkers(4))
		assert.NoError(err)
		assert.NotNil(pool)
	})

	t.Run("WithWorkerName", func(t *testing.T) {
		pool, err := queue.NewWorkerPool(mgr, queue.WithWorkerName("test-worker"))
		assert.NoError(err)
		assert.NotNil(pool)
	})

	t.Run("InvalidWorkers", func(t *testing.T) {
		_, err := queue.NewWorkerPool(mgr, queue.WithWorkers(0))
		assert.Error(err)
		assert.ErrorIs(err, queue.ErrInvalidWorkers)
	})

	t.Run("InvalidPeriod", func(t *testing.T) {
		_, err := queue.NewWorkerPool(mgr, queue.WithPeriod(100*time.Microsecond))
		assert.Error(err)
		assert.ErrorIs(err, queue.ErrInvalidPeriod)
	})
}

func Test_WorkerPool_RegisterQueue(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_wp_queue"))
	assert.NoError(err)

	pool, err := queue.NewWorkerPool(mgr)
	assert.NoError(err)

	t.Run("RegisterQueue", func(t *testing.T) {
		handler := func(ctx context.Context, task *schema.Task) error {
			return nil
		}

		q, err := pool.RegisterQueue(ctx, schema.QueueMeta{
			Queue: "test-queue",
		}, handler)
		assert.NoError(err)
		assert.NotNil(q)
		assert.Equal("test-queue", q.Queue)
	})
}

func Test_WorkerPool_RegisterTicker(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_wp_ticker"))
	assert.NoError(err)

	pool, err := queue.NewWorkerPool(mgr)
	assert.NoError(err)

	t.Run("RegisterTicker", func(t *testing.T) {
		handler := func(ctx context.Context, ticker *schema.Ticker) error {
			return nil
		}

		interval := time.Minute
		ticker, err := pool.RegisterTicker(ctx, schema.TickerMeta{
			Ticker:   "test-ticker",
			Interval: &interval,
		}, handler)
		assert.NoError(err)
		assert.NotNil(ticker)
		assert.Equal("test-ticker", ticker.Ticker)
	})
}

func Test_WorkerPool_Run(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_wp_run"))
	assert.NoError(err)

	t.Run("RunWithNoHandlers", func(t *testing.T) {
		pool, err := queue.NewWorkerPool(mgr)
		assert.NoError(err)

		runCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		err = pool.Run(runCtx)
		assert.NoError(err)
	})

	t.Run("RunProcessesTask", func(t *testing.T) {
		pool, err := queue.NewWorkerPool(mgr, queue.WithWorkers(1))
		assert.NoError(err)

		var processed atomic.Int32
		handler := func(ctx context.Context, task *schema.Task) error {
			processed.Add(1)
			return nil
		}

		_, err = pool.RegisterQueue(ctx, schema.QueueMeta{
			Queue: "process-queue",
		}, handler)
		assert.NoError(err)

		// Create a task with payload
		_, err = mgr.CreateTask(ctx, "process-queue", schema.TaskMeta{
			Payload: map[string]any{"test": true},
		})
		assert.NoError(err)

		// Run with timeout
		runCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()

		err = pool.Run(runCtx)
		assert.NoError(err)
		assert.GreaterOrEqual(processed.Load(), int32(1))
	})

	t.Run("RunProcessesTicker", func(t *testing.T) {
		pool, err := queue.NewWorkerPool(mgr, queue.WithPeriod(10*time.Millisecond))
		assert.NoError(err)

		var processed atomic.Int32
		handler := func(ctx context.Context, ticker *schema.Ticker) error {
			processed.Add(1)
			return nil
		}

		interval := 10 * time.Millisecond
		_, err = pool.RegisterTicker(ctx, schema.TickerMeta{
			Ticker:   "process-ticker",
			Interval: &interval,
		}, handler)
		assert.NoError(err)

		// Run with timeout
		runCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		err = pool.Run(runCtx)
		assert.NoError(err)
		assert.GreaterOrEqual(processed.Load(), int32(1))
	})

	t.Run("RunParallelTasks", func(t *testing.T) {
		const numWorkers = 4
		const numTasks = 4
		const taskDuration = 100 * time.Millisecond

		pool, err := queue.NewWorkerPool(mgr, queue.WithWorkers(numWorkers))
		assert.NoError(err)

		var processed atomic.Int32
		allDone := make(chan struct{})
		handler := func(ctx context.Context, task *schema.Task) error {
			time.Sleep(taskDuration)
			if processed.Add(1) == numTasks {
				close(allDone)
			}
			return nil
		}

		_, err = pool.RegisterQueue(ctx, schema.QueueMeta{
			Queue: "parallel-queue",
		}, handler)
		assert.NoError(err)

		// Create multiple tasks
		for i := 0; i < numTasks; i++ {
			_, err = mgr.CreateTask(ctx, "parallel-queue", schema.TaskMeta{
				Payload: map[string]any{"i": i},
			})
			assert.NoError(err)
		}

		// Run in background and measure time until all tasks are done
		runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		start := time.Now()
		go func() {
			pool.Run(runCtx)
		}()

		// Wait for all tasks to complete
		select {
		case <-allDone:
			// All tasks done
		case <-runCtx.Done():
			t.Fatal("Timeout waiting for tasks to complete")
		}
		elapsed := time.Since(start)
		cancel() // Stop the worker pool

		assert.Equal(int32(numTasks), processed.Load())

		// With 4 workers and 4 tasks of 100ms each:
		// Sequential: ~400ms, Parallel: ~100ms
		// Allow some margin, but should be well under 300ms
		assert.Less(elapsed, 300*time.Millisecond, "tasks should run in parallel")
		t.Logf("Processed %d tasks in %v (expected ~%v parallel)", numTasks, elapsed, taskDuration)
	})
}
