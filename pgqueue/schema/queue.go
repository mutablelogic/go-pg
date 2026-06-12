package schema

import (
	"strings"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
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
}

type Queue struct {
	Queue string `json:"queue,omitempty" arg:"" help:"Queue name"`
	QueueMeta
}

type QueueListRequest struct {
	pg.OffsetLimit
}

type QueueList struct {
	QueueListRequest
	Count uint64  `json:"count"`
	Body  []Queue `json:"body,omitempty"`
}

type QueueCleanRequest struct {
	Queue string `json:"queue,omitempty" arg:"" help:"Queue name"`
}

type QueueCleanResponse struct {
	QueueCleanRequest
	Body []Task `json:"body,omitempty"`
}

type QueueStatus struct {
	Queue  string `json:"queue"`
	Status string `json:"status"`
	Count  uint64 `json:"count"`
}

type QueueStatusRequest struct{}

type QueueStatusResponse struct {
	QueueStatusRequest
	Body []QueueStatus `json:"body,omitempty"`
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

func (q QueueList) String() string {
	return types.Stringify(q)
}

func (q QueueCleanRequest) String() string {
	return types.Stringify(q)
}

func (q QueueCleanResponse) String() string {
	return types.Stringify(q)
}

func (q QueueStatus) String() string {
	return types.Stringify(q)
}

func (q QueueStatusResponse) String() string {
	return types.Stringify(q)
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (q *Queue) Scan(row pg.Row) error {
	return row.Scan(&q.Queue, &q.TTL, &q.Retries, &q.RetryDelay, &q.Concurrency)
}

func (l *QueueList) Scan(row pg.Row) error {
	var queue Queue
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

func (s *QueueStatus) Scan(row pg.Row) error {
	return row.Scan(&s.Queue, &s.Status, &s.Count)
}

func (l *QueueStatusResponse) Scan(row pg.Row) error {
	var status QueueStatus
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

	switch op {
	case pg.List:
		return bind.Query("pgqueue.clean"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported QueueCleanRequest operation %q", op)
	}
}

func (l QueueListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
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
	var where []string

	if bind.Has("id") {
		where = append(where, `q."queue" = @id`)
	}

	if len(where) > 0 {
		bind.Set("where", "AND "+strings.Join(where, " AND "))
	} else {
		bind.Set("where", "")
	}

	switch op {
	case pg.List:
		return bind.Query("pgqueue.stats"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported QueueStatusRequest operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

func (q Queue) Insert(bind *pg.Bind) (string, error) {
	queue, err := QueueName(q.Queue).queueName()
	if err != nil {
		return "", err
	}
	bind.Set("queue", queue)

	return bind.Query("pgqueue.insert"), nil
}

func (q Queue) Update(bind *pg.Bind) error {
	var patch []string

	if q.Queue != "" {
		queue, err := QueueName(q.Queue).queueName()
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
