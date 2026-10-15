package schema

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	tui "github.com/mutablelogic/go-server/pkg/tui"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type TaskId uint64

type TaskRetain struct {
	Queues []string `json:"queues,omitempty"`
	Worker string   `json:"worker,omitempty"`
}

type TaskRelease struct {
	Id     uint64          `json:"id,omitempty"`
	Fail   bool            `json:"fail,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
}

type TaskMeta struct {
	Payload   json.RawMessage `json:"payload,omitempty"`
	DelayedAt *time.Time      `json:"delayed_at,omitempty"`
}

type Task struct {
	Id     uint64          `json:"id,omitempty"`
	Queue  string          `json:"queue,omitempty"`
	Worker *string         `json:"worker,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	TaskMeta
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
	Queue  []string `json:"queue,omitempty" help:"Returns only the tasks in these queues, or in any queue" optional:""`
	Status string   `json:"status,omitempty" help:"Returns only the tasks with this status" enum:"new,running,retry,expired,failed,done" optional:""`
}

type TaskList struct {
	TaskListRequest
	Count uint64           `json:"count"`
	Body  []TaskWithStatus `json:"body,omitempty"`
}

type TaskFunc func(context.Context, json.RawMessage) (any, error)

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (t Task) String() string {
	return types.Stringify(t)
}

func (t TaskMeta) String() string {
	return types.Stringify(t)
}

func (t TaskWithStatus) String() string {
	return types.Stringify(t)
}

func (t TaskList) String() string {
	return types.Stringify(t)
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (t *TaskId) Scan(row pg.Row) error {
	var id *uint64
	if err := row.Scan(&id); err != nil {
		return err
	}
	*t = TaskId(types.Value(id))
	return nil
}

func (t *Task) Scan(row pg.Row) error {
	return row.Scan(
		&t.Id,
		&t.Queue,
		&t.Payload,
		&t.Result,
		&t.Worker,
		&t.CreatedAt,
		&t.DelayedAt,
		&t.StartedAt,
		&t.FinishedAt,
		&t.DiesAt,
		&t.Retries,
	)
}

func (t *TaskWithStatus) Scan(row pg.Row) error {
	return row.Scan(
		&t.Id,
		&t.Queue,
		&t.Payload,
		&t.Result,
		&t.Worker,
		&t.CreatedAt,
		&t.DelayedAt,
		&t.StartedAt,
		&t.FinishedAt,
		&t.DiesAt,
		&t.Retries,
		&t.Status,
	)
}

func (l *TaskList) Scan(row pg.Row) error {
	var task TaskWithStatus
	if err := task.Scan(row); err != nil {
		return err
	}
	l.Body = append(l.Body, task)
	return nil
}

func (l *TaskList) ScanCount(row pg.Row) error {
	return row.Scan(&l.Count)
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

func (t TaskMeta) Insert(bind *pg.Bind) (string, error) {
	if !bind.Has("id") {
		return "", httpresponse.ErrBadRequest.With("missing queue id")
	}
	bind.Set("queue", bind.Get("id"))

	if t.Payload == nil {
		return "", httpresponse.ErrBadRequest.With("missing payload")
	}
	bind.Set("payload", string(t.Payload))

	if t.DelayedAt != nil {
		if t.DelayedAt.Before(time.Now()) {
			return "", httpresponse.ErrBadRequest.With("delayed_at is in the past")
		}
		bind.Set("delayed_at", t.DelayedAt.UTC())
	} else {
		bind.Set("delayed_at", nil)
	}

	return bind.Query("pgqueue.task_insert"), nil
}

func (t TaskMeta) Update(bind *pg.Bind) error {
	bind.Del("patch")

	if t.DelayedAt != nil {
		if t.DelayedAt.Before(time.Now()) {
			return httpresponse.ErrBadRequest.With("delayed_at is in the past")
		}
		bind.Append("patch", `delayed_at = `+bind.Set("delayed_at", t.DelayedAt.UTC()))
	}

	if t.Payload != nil {
		bind.Append("patch", `payload = CAST(`+bind.Set("payload", string(t.Payload))+` AS JSONB)`)
	}

	if patch := bind.Join("patch", ", "); patch == "" {
		return httpresponse.ErrBadRequest.With("no fields to update")
	} else {
		bind.Set("patch", patch)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////
// TABLE

func (t TaskWithStatus) Header() []string {
	return []string{"ID", "Queue", "Status", "Retries", "Worker", "Created", "Payload", "Result"}
}

func (t TaskWithStatus) Width(col int) int {
	return 0
}

func (t TaskWithStatus) Cell(col int) tui.CellText {
	switch col {
	case 0:
		return strconv.FormatUint(t.Id, 10)
	case 1:
		return t.Queue
	case 2:
		return t.Status
	case 3:
		return countCell(t.Retries)
	case 4:
		return types.Value(t.Worker)
	case 5:
		return timeCell(t.CreatedAt)
	case 6:
		return jsonCell(t.Payload)
	case 7:
		return jsonCell(t.Result)
	default:
		return ""
	}
}

////////////////////////////////////////////////////////////////////////////////
// QUERY

// Query returns the request as URL query parameters
func (t TaskListRequest) Query() url.Values {
	values := url.Values{}
	t.OffsetLimit.Query(values)
	for _, queue := range t.Queue {
		if queue = strings.TrimSpace(queue); queue != "" {
			values.Add("queue", queue)
		}
	}
	if status := strings.TrimSpace(t.Status); status != "" {
		values.Set("status", status)
	}
	return values
}

////////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (t TaskId) Select(bind *pg.Bind, op pg.Op) (string, error) {
	bind.Set("tid", t)

	switch op {
	case pg.Get:
		return bind.Query("pgqueue.task_get"), nil
	case pg.Update:
		return bind.Query("pgqueue.task_patch"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported TaskId operation %q", op)
	}
}

func (l TaskListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	var taskWhere []string
	var where []string

	if bind.Has("id") {
		taskWhere = append(taskWhere, `t."queue" = @id`)
	}
	if queues, err := l.queues(); err != nil {
		return "", err
	} else if len(queues) > 0 {
		taskWhere = append(taskWhere, `t."queue" = ANY(`+bind.Set("queues", queues)+`)`)
	}
	if status, err := l.status(); err != nil {
		return "", err
	} else if status != "" {
		where = append(where, `"status" = `+bind.Set("status", status))
	}

	if len(taskWhere) == 0 {
		bind.Set("taskwhere", "")
	} else {
		bind.Set("taskwhere", "WHERE "+strings.Join(taskWhere, " AND "))
	}
	if len(where) == 0 {
		bind.Set("where", "")
	} else {
		bind.Set("where", "WHERE "+strings.Join(where, " AND "))
	}

	l.OffsetLimit.Bind(bind, QueueListLimit)

	switch op {
	case pg.List:
		return bind.Query("pgqueue.task_list"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported TaskListRequest operation %q", op)
	}
}

// queues returns the names of the queues to list the tasks of, which are
// valid queue names, or none for any queue
func (l TaskListRequest) queues() ([]string, error) {
	queues := make([]string, 0, len(l.Queue))
	for _, queue := range l.Queue {
		if strings.TrimSpace(queue) == "" {
			continue
		}
		name, err := QueueName(queue).queueName()
		if err != nil {
			return nil, err
		}
		queues = append(queues, name)
	}
	return queues, nil
}

// status returns the status of the tasks to list, or an empty string for any
// status
func (l TaskListRequest) status() (string, error) {
	switch status := strings.ToLower(strings.TrimSpace(l.Status)); status {
	case "":
		return "", nil
	case "new", "running", "retry", "expired", "failed", "done":
		return status, nil
	default:
		return "", httpresponse.ErrBadRequest.Withf("invalid task status: %q", l.Status)
	}
}

func (t TaskRetain) Select(bind *pg.Bind, op pg.Op) (string, error) {
	queues := make([]string, 0, len(t.Queues))
	for _, queue := range t.Queues {
		if strings.TrimSpace(queue) == "" {
			continue
		}
		name, err := QueueName(queue).queueName()
		if err != nil {
			return "", err
		}
		queues = append(queues, name)
	}
	bind.Set("queues", queues)

	if worker := strings.TrimSpace(t.Worker); worker == "" {
		return "", httpresponse.ErrBadRequest.With("missing worker")
	} else {
		bind.Set("worker", worker)
	}

	switch op {
	case pg.Get:
		return bind.Query("pgqueue.retain"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported TaskRetain operation %q", op)
	}
}

func (t TaskRelease) Select(bind *pg.Bind, op pg.Op) (string, error) {
	if t.Id == 0 {
		return "", httpresponse.ErrBadRequest.With("missing task id")
	}
	bind.Set("tid", t.Id)

	if len(t.Result) == 0 {
		bind.Set("result", "null")
	} else {
		bind.Set("result", string(t.Result))
	}

	switch op {
	case pg.Get:
		if t.Fail {
			return bind.Query("pgqueue.fail"), nil
		}
		return bind.Query("pgqueue.release"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported TaskRelease operation %q", op)
	}
}
