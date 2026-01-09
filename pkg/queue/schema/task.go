package schema

import (
	"encoding/json"
	"strings"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type TaskId uint64

type TaskRetain struct {
	Queue  string `json:"queue,omitempty"`
	Worker string `json:"worker,omitempty"`
}

type TaskRelease struct {
	Id     uint64 `json:"id,omitempty"`
	Fail   bool   `json:"fail,omitempty"`
	Result any    `json:"result,omitempty"`
}

type TaskMeta struct {
	Payload   any        `json:"payload,omitempty"`
	DelayedAt *time.Time `json:"delayed_at,omitempty"`
}

type Task struct {
	Id uint64 `json:"id,omitempty"`
	TaskMeta
	Worker     *string    `json:"worker,omitempty"`
	Namespace  string     `json:"namespace,omitempty"`
	Queue      string     `json:"queue,omitempty"`
	Result     any        `json:"result,omitempty"`
	CreatedAt  *time.Time `json:"created_at,omitempty"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	DiesAt     *time.Time `json:"dies_at,omitempty"`
	Retries    *uint64    `json:"retries,omitempty"`
}

type TaskWithStatus struct {
	Task
	Status string `json:"status,omitempty"`
}

type TaskListRequest struct {
	pg.OffsetLimit
	Status string `json:"status,omitempty"`
}

type TaskList struct {
	TaskListRequest
	Count uint64           `json:"count"`
	Body  []TaskWithStatus `json:"body,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (t Task) String() string {
	return stringify(t)
}

func (t TaskMeta) String() string {
	return stringify(t)
}

func (t TaskWithStatus) String() string {
	return stringify(t)
}

func (t TaskList) String() string {
	return stringify(t)
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (t *TaskId) Scan(row pg.Row) error {
	var id *uint64
	if err := row.Scan(&id); err != nil {
		return err
	} else {
		*t = TaskId(types.PtrUint64(id))
	}
	return nil
}

func (t *Task) Scan(row pg.Row) error {
	return row.Scan(&t.Id, &t.Queue, &t.Namespace, &t.Payload, &t.Result, &t.Worker, &t.CreatedAt, &t.DelayedAt, &t.StartedAt, &t.FinishedAt, &t.DiesAt, &t.Retries)
}

func (t *TaskWithStatus) Scan(row pg.Row) error {
	return row.Scan(&t.Id, &t.Queue, &t.Namespace, &t.Payload, &t.Result, &t.Worker, &t.CreatedAt, &t.DelayedAt, &t.StartedAt, &t.FinishedAt, &t.DiesAt, &t.Retries, &t.Status)
}

// TaskList
func (l *TaskList) Scan(row pg.Row) error {
	var task TaskWithStatus
	if err := task.Scan(row); err != nil {
		return err
	}
	l.Body = append(l.Body, task)
	return nil
}

// TaskListCount
func (l *TaskList) ScanCount(row pg.Row) error {
	return row.Scan(&l.Count)
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

func (t TaskMeta) Insert(bind *pg.Bind) (string, error) {
	if !bind.Has("id") {
		return "", httpresponse.ErrBadRequest.With("missing queue id")
	} else {
		bind.Set("queue", bind.Get("id"))
	}
	if t.Payload == nil {
		return "", httpresponse.ErrBadRequest.With("missing payload")
	} else if data, err := json.Marshal(t.Payload); err != nil {
		return "", err
	} else {
		bind.Set("payload", string(data))
	}
	if t.DelayedAt != nil {
		if t.DelayedAt.Before(time.Now()) {
			return "", httpresponse.ErrBadRequest.With("delayed_at is in the past")
		}
		bind.Set("delayed_at", t.DelayedAt.UTC())
	} else {
		bind.Set("delayed_at", nil)
	}
	return bind.Replace("${pgqueue.task_insert}"), nil
}

func (t TaskMeta) Update(bind *pg.Bind) error {
	bind.Del("patch")

	// DelayedAt
	if t.DelayedAt != nil {
		if t.DelayedAt.Before(time.Now()) {
			return httpresponse.ErrBadRequest.With("delayed_at is in the past")
		}
		bind.Append("patch", `delayed_at = `+bind.Set("delayed_at", t.DelayedAt))
	}

	// Payload
	if t.Payload != nil {
		data, err := json.Marshal(t.Payload)
		if err != nil {
			return err
		}
		bind.Append("patch", `payload = `+bind.Set("payload", string(data)))
	}

	// Set patch
	if patch := bind.Join("patch", ", "); patch == "" {
		return httpresponse.ErrBadRequest.With("no fields to update")
	} else {
		bind.Set("patch", patch)
	}

	// Return success
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (t TaskId) Select(bind *pg.Bind, op pg.Op) (string, error) {
	bind.Set("tid", t)
	switch op {
	case pg.Get:
		return bind.Replace("${pgqueue.task_get}"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported TaskId operation %q", op)
	}
}

func (l TaskListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Bind parameters
	var where []string
	if l.Status != "" {
		where = append(where, `status=`+bind.Set("status", l.Status))
	}
	if len(where) == 0 {
		bind.Set("where", "")
	} else {
		bind.Set("where", "WHERE "+strings.Join(where, " AND "))
	}
	l.OffsetLimit.Bind(bind, QueueListLimit)

	switch op {
	case pg.List:
		return bind.Replace("${pgqueue.task_list}"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported TaskListRequest operation %q", op)
	}
}

func (t TaskRetain) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Queue is required
	if queue := strings.TrimSpace(t.Queue); queue == "" {
		return "", httpresponse.ErrBadRequest.Withf("Missing queue")
	} else {
		bind.Set("queue", queue)
	}

	// Worker is required
	if worker := strings.TrimSpace(t.Worker); worker == "" {
		return "", httpresponse.ErrBadRequest.Withf("Missing worker")
	} else {
		bind.Set("worker", worker)
	}

	// Retain
	switch op {
	case pg.Get:
		return bind.Replace("${pgqueue.retain}"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported TaskRetain operation %q", op)
	}
}

func (t TaskRelease) Select(bind *pg.Bind, op pg.Op) (string, error) {
	if t.Id == 0 {
		return "", httpresponse.ErrBadRequest.Withf("Missing task id")
	} else {
		bind.Set("tid", t.Id)
	}

	// Result of the task
	data, err := json.Marshal(t.Result)
	if err != nil {
		return "", err
	} else {
		bind.Set("result", string(data))
	}

	// Release
	switch op {
	case pg.Get:
		if t.Fail {
			return bind.Replace("${pgqueue.fail}"), nil
		} else {
			return bind.Replace("${pgqueue.release}"), nil
		}
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported TaskRelease operation %q", op)
	}
}
