/*
Package queue provides a PostgreSQL-backed task queue with support for delayed
tasks, retries with exponential backoff, and periodic tickers.

# Manager

Create a manager with namespace isolation:

	mgr, err := queue.New(ctx, pool, "myapp")
	if err != nil {
		panic(err)
	}

# Queues

Register queues that define retry behavior:

	ttl := 24 * time.Hour
	retries := uint64(3)
	retryDelay := time.Minute

	queue, err := mgr.RegisterQueue(ctx, schema.QueueMeta{
		Queue:      "emails",
		TTL:        &ttl,
		Retries:    &retries,
		RetryDelay: &retryDelay,
	})

# Tasks

Create and process tasks:

	// Create a task
	task, err := mgr.CreateTask(ctx, "emails", schema.TaskMeta{
		Payload: map[string]any{"to": "user@example.com"},
	})

	// Retain next task from a specific queue
	task, err := mgr.NextTask(ctx, "worker-1", "emails")

	// Retain next task from any queue
	task, err := mgr.NextTask(ctx, "worker-1")

	// Retain next task from multiple queues
	task, err := mgr.NextTask(ctx, "worker-1", "emails", "notifications")

	// Release task (success)
	mgr.ReleaseTask(ctx, task.Id, true, nil, &status)

	// Release task (failure - will retry with backoff)
	mgr.ReleaseTask(ctx, task.Id, false, errPayload, &status)

# WorkerPool

Use WorkerPool for concurrent task processing:

	pool, err := queue.NewWorkerPool(mgr,
		queue.WithWorkers(4),
		queue.WithName("worker-1"),
	)

	// Register queue handlers
	pool.RegisterQueue(ctx, schema.QueueMeta{Queue: "emails"}, func(ctx context.Context, task *schema.Task) error {
		// Process task
		return nil
	})

	// Run blocks until context is cancelled
	err = pool.Run(ctx)

# Tickers

Register periodic tickers:

	interval := time.Hour
	ticker, err := mgr.RegisterTicker(ctx, schema.TickerMeta{
		Ticker:   "hourly-report",
		Interval: &interval,
	})

# Subpackages

  - schema: Data types, request/response structures, and SQL generation
  - httphandler: REST API handlers for all queue operations
  - httpclient: Typed Go client for the REST API
*/
package queue
