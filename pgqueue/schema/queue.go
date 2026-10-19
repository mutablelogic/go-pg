package schema

import (
	"fmt"
	"net/url"
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

type QueueName string

type QueueMeta struct {
	TTL         *time.Duration `json:"ttl,omitempty" help:"Time-to-live for queue messages"`
	Retries     *uint64        `json:"retries" help:"Number of retries before failing"`
	RetryDelay  *time.Duration `json:"retry_delay" help:"Backoff delay"`
	Concurrency *uint64        `json:"concurrency" help:"Number of concurrent workers"`
	Dedupe      *bool          `json:"dedupe,omitempty" help:"Whether a task is not inserted while the same task is pending"`
}

type Queue struct {
	Name string `json:"queue,omitempty" arg:"" help:"Queue name"`
	QueueMeta
}

type QueueListRequest struct {
	pg.OffsetLimit
}

// QueueWithStatus is a queue with the number of its tasks with each status
type QueueWithStatus struct {
	Queue
	New     uint64 `json:"new" help:"Tasks which are ready to run"`
	Running uint64 `json:"running" help:"Tasks which are running"`
	Retry   uint64 `json:"retry" help:"Tasks which are waiting to be retried"`
	Expired uint64 `json:"expired" help:"Tasks which ran for longer than the queue's ttl"`
	Failed  uint64 `json:"failed" help:"Tasks which failed with no retries left"`
	Done    uint64 `json:"done" help:"Tasks which are done"`
}

type QueueList struct {
	QueueListRequest
	Count uint64            `json:"count"`
	Body  []QueueWithStatus `json:"body,omitempty"`
}

type QueueCleanRequest struct {
	Queue     string        `json:"queue,omitempty" arg:"" help:"Queue name"`
	Retention time.Duration `json:"retention,omitempty" help:"How long finished tasks are kept"`
}

type QueueCleanResponse struct {
	QueueCleanRequest
	Body []Task `json:"body,omitempty"`
}

// QueueStatusRequest gets a queue with the number of its tasks with each
// status, or lists every queue with them
type QueueStatusRequest struct {
	Queue string `json:"queue,omitempty"`
}

type QueueStatusResponse struct {
	QueueStatusRequest
	Body []QueueWithStatus `json:"body,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (q Queue) String() string {
	return types.Stringify(q)
}

func (q QueueMeta) String() string {
	return types.Stringify(q)
}

func (q QueueListRequest) String() string {
	return types.Stringify(q)
}

////////////////////////////////////////////////////////////////////////////////
// TABLE

func (q Queue) Header() []string {
	return []string{"Queue", "TTL", "Retries", "Retry Delay", "Concurrency", "Dedupe"}
}

func (q Queue) Width(col int) int {
	return 0
}

func (q Queue) Cell(col int) tui.CellText {
	switch col {
	case 0:
		return q.Name
	case 1:
		return durationCell(q.TTL)
	case 2:
		return countCell(q.Retries)
	case 3:
		return durationCell(q.RetryDelay)
	case 4:
		if q.Concurrency != nil && *q.Concurrency == 0 {
			return "unlimited"
		}
		return countCell(q.Concurrency)
	case 5:
		if q.Dedupe != nil && *q.Dedupe {
			return "yes"
		}
		return ""
	default:
		return ""
	}
}

func (q QueueWithStatus) Header() []string {
	return append(q.Queue.Header(), "New", "Running", "Retry", "Expired", "Failed", "Done")
}

func (q QueueWithStatus) Width(col int) int {
	return 0
}

func (q QueueWithStatus) Cell(col int) tui.CellText {
	counts := []uint64{q.New, q.Running, q.Retry, q.Expired, q.Failed, q.Done}
	if n := len(q.Queue.Header()); col < n {
		return q.Queue.Cell(col)
	} else if col-n < len(counts) {
		return fmt.Sprint(counts[col-n])
	}
	return ""
}

////////////////////////////////////////////////////////////////////////////////
// QUERY

// Query returns the request as URL query parameters
func (q QueueListRequest) Query() url.Values {
	values := url.Values{}
	q.OffsetLimit.Query(values)
	return values
}

func (q QueueList) String() string {
	return types.Stringify(q)
}

func (q QueueCleanRequest) String() string {
	return types.Stringify(q)
}

func (q QueueCleanResponse) String() string {
	return types.Stringify(q)
}

func (q QueueWithStatus) String() string {
	return types.Stringify(q)
}

func (q QueueStatusResponse) String() string {
	return types.Stringify(q)
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (q *Queue) Scan(row pg.Row) error {
	return row.Scan(&q.Name, &q.TTL, &q.Retries, &q.RetryDelay, &q.Concurrency, &q.Dedupe)
}

func (q *QueueWithStatus) Scan(row pg.Row) error {
	return row.Scan(
		&q.Name, &q.TTL, &q.Retries, &q.RetryDelay, &q.Concurrency, &q.Dedupe,
		&q.New, &q.Running, &q.Retry, &q.Expired, &q.Failed, &q.Done,
	)
}

func (l *QueueList) Scan(row pg.Row) error {
	var queue QueueWithStatus
	if err := queue.Scan(row); err != nil {
		return err
	}
	l.Body = append(l.Body, queue)
	return nil
}

func (l *QueueList) ScanCount(row pg.Row) error {
	return row.Scan(&l.Count)
}

func (l *QueueCleanResponse) Scan(row pg.Row) error {
	var task Task
	if err := row.Scan(
		&task.Id,
		&task.Queue,
		&task.Payload,
		&task.Result,
		&task.Worker,
		&task.CreatedAt,
		&task.DelayedAt,
		&task.StartedAt,
		&task.FinishedAt,
		&task.DiesAt,
		&task.Retries,
	); err != nil {
		return err
	}
	l.Body = append(l.Body, task)
	return nil
}

func (l *QueueStatusResponse) Scan(row pg.Row) error {
	var status QueueWithStatus
	if err := status.Scan(row); err != nil {
		return err
	}
	l.Body = append(l.Body, status)
	return nil
}

////////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (q QueueName) Select(bind *pg.Bind, op pg.Op) (string, error) {
	name, err := q.queueName()
	if err != nil {
		return "", err
	}
	bind.Set("id", name)

	switch op {
	case pg.Get:
		return bind.Query("pgqueue.get"), nil
	case pg.Update:
		return bind.Query("pgqueue.patch"), nil
	case pg.Delete:
		return bind.Query("pgqueue.delete"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported QueueName operation %q", op)
	}
}

func (q QueueCleanRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	name, err := QueueName(q.Queue).queueName()
	if err != nil {
		return "", err
	}
	bind.Set("id", name)
	bind.Set("retention", max(q.Retention, 0))

	switch op {
	case pg.List:
		return bind.Query("pgqueue.clean"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported QueueCleanRequest operation %q", op)
	}
}

func (l *QueueListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	bind.Set("where", "")
	l.OffsetLimit.Bind(bind, QueueListLimit)

	switch op {
	case pg.List:
		return bind.Query("pgqueue.list"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported QueueListRequest operation %q", op)
	}
}

func (l QueueStatusRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	switch op {
	case pg.Get:
		// The queue with the name
		name, err := QueueName(l.Queue).queueName()
		if err != nil {
			return "", err
		}
		bind.Set("id", name)
		return bind.Query("pgqueue.get_status"), nil
	case pg.List:
		// Every queue
		return bind.Query("pgqueue.stats"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported QueueStatusRequest operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

func (q Queue) Insert(bind *pg.Bind) (string, error) {
	queue, err := QueueName(q.Name).queueName()
	if err != nil {
		return "", err
	}
	bind.Set("queue", queue)

	return bind.Query("pgqueue.insert"), nil
}

func (q Queue) Update(bind *pg.Bind) error {
	var patch []string

	if q.Name != "" {
		queue, err := QueueName(q.Name).queueName()
		if err != nil {
			return err
		}
		patch = append(patch, `queue=`+bind.Set("queue", queue))
	}
	if q.TTL != nil {
		patch = append(patch, `ttl=`+bind.Set("ttl", q.TTL))
	}
	if q.Retries != nil {
		patch = append(patch, `retries=`+bind.Set("retries", q.Retries))
	}
	if q.RetryDelay != nil {
		patch = append(patch, `retry_delay=`+bind.Set("retry_delay", q.RetryDelay))
	}
	if q.Concurrency != nil {
		patch = append(patch, `concurrency=`+bind.Set("concurrency", q.Concurrency))
	}
	if q.Dedupe != nil {
		patch = append(patch, `dedupe=`+bind.Set("dedupe", q.Dedupe))
	}

	if len(patch) == 0 {
		return httpresponse.ErrBadRequest.With("no patch values")
	}
	bind.Set("patch", strings.Join(patch, ", "))

	return nil
}

func (q QueueMeta) Update(bind *pg.Bind) error {
	return Queue{QueueMeta: q}.Update(bind)
}

func (q QueueName) queueName() (string, error) {
	if queue := strings.ToLower(strings.TrimSpace(string(q))); !types.IsIdentifier(queue) {
		return "", httpresponse.ErrBadRequest.Withf("invalid queue name: %q", queue)
	} else {
		return queue, nil
	}
}
