package pgqueue

import (
	"context"
	"encoding/json"
	"errors"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS - TASK

// CreateTask creates a new task in a queue, and returns it.
func (self *Manager) CreateTask(ctx context.Context, queue string, meta schema.TaskMeta) (result *schema.Task, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.CreateTask",
		attribute.String("queue", queue),
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	var taskId schema.TaskId
	var task schema.TaskWithStatus

	if err := self.Tx(ctx, func(conn pg.Conn) error {
		if err := conn.With("id", queue).Insert(ctx, &taskId, meta); err != nil {
			return err
		}
		return conn.Get(ctx, &task, taskId)
	}); err != nil {
		return nil, err
	}

	return types.Ptr(task.Task), nil
}

// ListTasks returns all tasks with optional queue and status filtering.
func (self *Manager) ListTasks(ctx context.Context, req schema.TaskListRequest) (result *schema.TaskList, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.ListTasks",
		attribute.String("req", types.Stringify(req)),
	)
	defer func() { endSpan(err) }()

	resp := schema.TaskList{TaskListRequest: req}
	if err := self.List(ctx, &resp, req); err != nil {
		return nil, err
	} else {
		resp.OffsetLimit.Clamp(resp.Count)
	}
	return types.Ptr(resp), nil
}

// NextTask retains a task from any of the specified queues, and returns it.
// If no task is available, nil is returned.
func (self *Manager) NextTask(ctx context.Context, worker string, queues ...string) (result *schema.Task, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.NextTask",
		attribute.String("worker", worker),
		attribute.StringSlice("queues", queues),
	)
	defer func() { endSpan(err) }()

	var taskId schema.TaskId
	var task schema.TaskWithStatus

	if err := self.Tx(ctx, func(conn pg.Conn) error {
		if err := conn.Get(ctx, &taskId, schema.TaskRetain{Queues: queues, Worker: worker}); errors.Is(err, pg.ErrNotFound) {
			return nil
		} else if err != nil {
			return err
		}

		if taskId == 0 {
			return nil
		}

		return conn.Get(ctx, &task, taskId)
	}); err != nil {
		return nil, err
	}

	if taskId == 0 {
		return nil, nil
	}

	return types.Ptr(task.Task), nil
}

// ReleaseTask releases a task, optionally marking it as failed, and returns it.
func (self *Manager) ReleaseTask(ctx context.Context, taskId uint64, success bool, result json.RawMessage, status *string) (resp *schema.Task, err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.ReleaseTask",
		attribute.Int64("task", int64(taskId)),
		attribute.Bool("success", success),
		attribute.String("result", string(result)),
		attribute.Bool("status", status != nil),
	)
	defer func() { endSpan(err) }()

	var released schema.TaskId
	var task schema.TaskWithStatus

	if err := self.Tx(ctx, func(conn pg.Conn) error {
		if err := conn.Get(ctx, &released, schema.TaskRelease{Id: taskId, Fail: !success, Result: result}); err != nil {
			return err
		}

		if released == 0 {
			return pg.ErrNotFound
		}

		return conn.Get(ctx, &task, released)
	}); err != nil {
		return nil, err
	}

	if status != nil {
		*status = task.Status
	}

	return types.Ptr(task.Task), nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// requeueTask puts a running task back, without using a retry, such as when
// the event loop stops before it finishes
func (self *Manager) requeueTask(ctx context.Context, taskId uint64) (err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgqueue.requeueTask",
		attribute.Int64("task", int64(taskId)),
	)
	defer func() { endSpan(err) }()

	var requeued schema.TaskId
	if err := self.Get(ctx, &requeued, schema.TaskRequeue{Id: taskId}); err != nil {
		return err
	} else if requeued == 0 {
		return pg.ErrNotFound
	}
	return nil
}
