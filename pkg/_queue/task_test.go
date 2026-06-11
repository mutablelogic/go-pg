package queue_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	// Packages
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	assert "github.com/stretchr/testify/assert"
)

////////////////////////////////////////////////////////////////////////////////
// TASK CRUD TESTS

func Test_Task_CreateTask(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_task_create"))
	assert.NoError(err)
	assert.NotNil(mgr)

	// Create a queue first
	q, err := mgr.RegisterQueue(ctx, schema.QueueMeta{Queue: "test-queue"})
	assert.NoError(err)
	assert.NotNil(q)

	t.Run("CreateTaskWithPayload", func(t *testing.T) {
		payload := json.RawMessage(`{"action":"process","data":"test"}`)

		task, err := mgr.CreateTask(ctx, "test-queue", schema.TaskMeta{
			Payload: payload,
		})

		assert.NoError(err)
		assert.NotNil(task)
		assert.NotZero(task.Id)
		assert.Equal("test-queue", task.Queue)
		assert.Equal("test_task_create", task.Namespace)
		assert.NotNil(task.CreatedAt)
		assert.Nil(task.Worker)
		assert.Nil(task.StartedAt)
		assert.Nil(task.FinishedAt)

		// Verify payload
		taskPayload, err := json.Marshal(task.Payload)
		assert.NoError(err)
		assert.JSONEq(string(payload), string(taskPayload))
	})

	t.Run("CreateTaskWithDelayedAt", func(t *testing.T) {
		payload := json.RawMessage(`{"delayed":true}`)
		delayedAt := time.Now().Add(5 * time.Minute)

		task, err := mgr.CreateTask(ctx, "test-queue", schema.TaskMeta{
			Payload:   payload,
			DelayedAt: &delayedAt,
		})

		assert.NoError(err)
		assert.NotNil(task)
		assert.NotNil(task.DelayedAt)
		assert.WithinDuration(delayedAt, *task.DelayedAt, time.Second)
	})

	t.Run("CreateTaskWithPastDelayedAt", func(t *testing.T) {
		payload := json.RawMessage(`{"test":true}`)
		pastTime := time.Now().Add(-1 * time.Hour)

		_, err := mgr.CreateTask(ctx, "test-queue", schema.TaskMeta{
			Payload:   payload,
			DelayedAt: &pastTime,
		})

		assert.Error(err)
		assert.Contains(err.Error(), "delayed_at is in the past")
	})

	t.Run("CreateTaskWithNilPayload", func(t *testing.T) {
		_, err := mgr.CreateTask(ctx, "test-queue", schema.TaskMeta{
			Payload: nil,
		})

		assert.Error(err)
		assert.Contains(err.Error(), "missing payload")
	})

	t.Run("CreateTaskNonExistentQueue", func(t *testing.T) {
		payload := json.RawMessage(`{"test":true}`)

		_, err := mgr.CreateTask(ctx, "nonexistent-queue", schema.TaskMeta{
			Payload: payload,
		})

		assert.Error(err)
	})
}

func Test_Task_NextTask(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_task_next"))
	assert.NoError(err)

	// Create queue
	_, err = mgr.RegisterQueue(ctx, schema.QueueMeta{Queue: "work-queue"})
	assert.NoError(err)

	t.Run("NextTaskFromEmptyQueue", func(t *testing.T) {
		task, err := mgr.NextTask(ctx, "worker-1", "work-queue")
		assert.NoError(err)
		assert.Nil(task)
	})

	t.Run("NextTaskAvailable", func(t *testing.T) {
		// Create a task
		payload := json.RawMessage(`{"job":"task1"}`)
		createdTask, err := mgr.CreateTask(ctx, "work-queue", schema.TaskMeta{
			Payload: payload,
		})
		assert.NoError(err)
		assert.NotNil(createdTask)

		// Get next task
		task, err := mgr.NextTask(ctx, "worker-1", "work-queue")
		assert.NoError(err)
		assert.NotNil(task)
		assert.Equal(createdTask.Id, task.Id)
		assert.NotNil(task.Worker)
		assert.Equal("worker-1", *task.Worker)
		assert.NotNil(task.StartedAt)
	})

	t.Run("NextTaskAlreadyRetained", func(t *testing.T) {
		// Create a task
		payload := json.RawMessage(`{"job":"task2"}`)
		_, err := mgr.CreateTask(ctx, "work-queue", schema.TaskMeta{
			Payload: payload,
		})
		assert.NoError(err)

		// First worker gets the task
		task1, err := mgr.NextTask(ctx, "worker-2", "work-queue")
		assert.NoError(err)
		assert.NotNil(task1)

		// Second worker should not get the same task
		task2, err := mgr.NextTask(ctx, "worker-3", "work-queue")
		assert.NoError(err)
		// Could be nil or a different task, but not the same one
		if task2 != nil {
			assert.NotEqual(task1.Id, task2.Id)
		}
	})

	t.Run("NextTaskWithDelayedTask", func(t *testing.T) {
		// Create a delayed task
		payload := json.RawMessage(`{"job":"delayed"}`)
		delayedAt := time.Now().Add(10 * time.Minute)
		_, err := mgr.CreateTask(ctx, "work-queue", schema.TaskMeta{
			Payload:   payload,
			DelayedAt: &delayedAt,
		})
		assert.NoError(err)

		// Should not get the delayed task
		task, err := mgr.NextTask(ctx, "worker-4", "work-queue")
		assert.NoError(err)
		// Task should be nil or a different non-delayed task
		if task != nil {
			assert.Nil(task.DelayedAt)
		}
	})
}

func Test_Task_ReleaseTask(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_task_release"))
	assert.NoError(err)

	// Create queue
	_, err = mgr.RegisterQueue(ctx, schema.QueueMeta{Queue: "release-queue"})
	assert.NoError(err)

	t.Run("ReleaseTaskSuccess", func(t *testing.T) {
		// Create and retain a task
		payload := json.RawMessage(`{"job":"release-success"}`)
		createdTask, err := mgr.CreateTask(ctx, "release-queue", schema.TaskMeta{
			Payload: payload,
		})
		assert.NoError(err)

		task, err := mgr.NextTask(ctx, "worker-release-1", "release-queue")
		assert.NoError(err)
		assert.NotNil(task)

		// Release task successfully
		result := map[string]any{"status": "completed", "count": 42}
		var status string
		releasedTask, err := mgr.ReleaseTask(ctx, task.Id, true, result, &status)

		assert.NoError(err)
		assert.NotNil(releasedTask)
		assert.Equal(createdTask.Id, releasedTask.Id)
		assert.NotNil(releasedTask.FinishedAt)
		// Status should be one of the completion states
		assert.Contains([]string{"finished", "released"}, status)
	})

	t.Run("ReleaseTaskFailure", func(t *testing.T) {
		// Create and retain a task
		payload := json.RawMessage(`{"job":"release-fail"}`)
		_, err := mgr.CreateTask(ctx, "release-queue", schema.TaskMeta{
			Payload: payload,
		})
		assert.NoError(err)

		task, err := mgr.NextTask(ctx, "worker-release-2", "release-queue")
		assert.NoError(err)
		assert.NotNil(task)

		// Release task with failure
		result := map[string]any{"error": "something went wrong"}
		var status string
		releasedTask, err := mgr.ReleaseTask(ctx, task.Id, false, result, &status)

		assert.NoError(err)
		assert.NotNil(releasedTask)
		// Status could be "pending", "retry" (if retries available) or "failed"
		assert.Contains([]string{"pending", "retry", "failed"}, status)
	})

	t.Run("ReleaseTaskWithoutStatus", func(t *testing.T) {
		// Create and retain a task
		payload := json.RawMessage(`{"job":"release-no-status"}`)
		_, err := mgr.CreateTask(ctx, "release-queue", schema.TaskMeta{
			Payload: payload,
		})
		assert.NoError(err)

		task, err := mgr.NextTask(ctx, "worker-release-3", "release-queue")
		assert.NoError(err)
		assert.NotNil(task)

		// Release without status parameter
		result := map[string]any{"completed": true}
		releasedTask, err := mgr.ReleaseTask(ctx, task.Id, true, result, nil)

		assert.NoError(err)
		assert.NotNil(releasedTask)
	})

	t.Run("ReleaseNonExistentTask", func(t *testing.T) {
		_, err := mgr.ReleaseTask(ctx, 999999, true, nil, nil)
		assert.Error(err)
	})

	t.Run("ReleaseTaskNotRetained", func(t *testing.T) {
		// Create a task but don't retain it
		payload := json.RawMessage(`{"job":"not-retained"}`)
		task, err := mgr.CreateTask(ctx, "release-queue", schema.TaskMeta{
			Payload: payload,
		})
		assert.NoError(err)

		// Try to release it without retaining first
		_, err = mgr.ReleaseTask(ctx, task.Id, true, nil, nil)
		// This might error or might succeed depending on implementation
		// The behavior depends on queue_unlock/queue_fail function logic
	})
}

func Test_Task_RunTaskLoop(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_task_loop"), queue.WithWorkerName("worker-loop"), queue.WithWorkers(2))
	assert.NoError(err)

	// Create queue
	_, err = mgr.RegisterQueue(ctx, schema.QueueMeta{Queue: "loop-queue"})
	assert.NoError(err)

	t.Run("TaskLoopProcessesTasks", func(t *testing.T) {
		loopCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		// Create a task
		payload := json.RawMessage(`{"job":"loop-task"}`)
		createdTask, err := mgr.CreateTask(ctx, "loop-queue", schema.TaskMeta{
			Payload: payload,
		})
		assert.NoError(err)

		// Track received tasks
		var receivedTask *schema.Task
		var mu sync.Mutex
		taskReceived := make(chan struct{}, 1)

		// Run loop in background
		errCh := make(chan error, 1)
		go func() {
			errCh <- mgr.RunTaskLoop(loopCtx, func(ctx context.Context, task *schema.Task) error {
				mu.Lock()
				receivedTask = task
				mu.Unlock()
				select {
				case taskReceived <- struct{}{}:
				default:
				}
				return nil
			}, "loop-queue")
		}()

		// Wait for task to be received
		select {
		case <-taskReceived:
			mu.Lock()
			assert.NotNil(receivedTask)
			assert.Equal(createdTask.Id, receivedTask.Id)
			mu.Unlock()
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for task")
		}

		// Wait for loop to finish
		cancel()
		err = <-errCh
		assert.NoError(err)
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		loopCtx, cancel := context.WithCancel(ctx)

		// Cancel immediately
		cancel()

		err := mgr.RunTaskLoop(loopCtx, func(ctx context.Context, task *schema.Task) error {
			return nil
		}, "loop-queue")
		assert.NoError(err)
	})

	t.Run("MultipleTasksFire", func(t *testing.T) {
		loopCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		// Create multiple tasks
		for i := 0; i < 3; i++ {
			payload := json.RawMessage(`{"job":"multi-task"}`)
			_, err := mgr.CreateTask(ctx, "loop-queue", schema.TaskMeta{
				Payload: payload,
			})
			assert.NoError(err)
		}

		// Track task count
		var taskCount int32

		// Run loop in background
		errCh := make(chan error, 1)
		go func() {
			errCh <- mgr.RunTaskLoop(loopCtx, func(ctx context.Context, task *schema.Task) error {
				atomic.AddInt32(&taskCount, 1)
				// Release task so loop continues
				_, _ = mgr.ReleaseTask(ctx, task.Id, true, nil, nil)
				return nil
			}, "loop-queue")
		}()

		// Wait a bit for tasks to be processed
		time.Sleep(1500 * time.Millisecond)

		cancel()
		<-errCh

		count := atomic.LoadInt32(&taskCount)
		assert.GreaterOrEqual(int(count), 3, "Should have processed at least 3 tasks")
		t.Logf("Processed %d tasks", count)
	})
}
