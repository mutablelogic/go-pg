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
	Queue      string         `json:"queue,omitempty" arg:"" help:"Queue name"`
	TTL        *time.Duration `json:"ttl,omitempty" help:"Time-to-live for queue messages"`
	Retries    *uint64        `json:"retries" help:"Number of retries before failing"`
	RetryDelay *time.Duration `json:"retry_delay" help:"Backoff delay"`
}

type Queue struct {
	QueueMeta
	Namespace string `json:"namespace,omitempty" help:"Namespace"`
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
	Body []Task `json:"body,omitempty"`
}

type QueueStatus struct {
	Queue  string `json:"queue"`
	Status string `json:"status"`
	Count  uint64 `json:"count"`
}

type QueueStatusRequest struct{}

type QueueStatusResponse struct {
	Body []QueueStatus `json:"body,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (q Queue) String() string {
	return stringify(q)
}

func (q QueueMeta) String() string {
	return stringify(q)
}

func (q QueueList) String() string {
	return stringify(q)
}

func (q QueueCleanResponse) String() string {
	return stringify(q)
}

func (q QueueStatus) String() string {
	return stringify(q)
}

////////////////////////////////////////////////////////////////////////////////
// READER

// Queue
func (q *Queue) Scan(row pg.Row) error {
	return row.Scan(&q.Queue, &q.TTL, &q.Retries, &q.RetryDelay, &q.Namespace)
}

// QueueList
func (l *QueueList) Scan(row pg.Row) error {
	var queue Queue
	if err := queue.Scan(row); err != nil {
		return err
	}
	l.Body = append(l.Body, queue)
	return nil
}

// QueueListCount
func (l *QueueList) ScanCount(row pg.Row) error {
	return row.Scan(&l.Count)
}

// QueueCleanResponse
func (l *QueueCleanResponse) Scan(row pg.Row) error {
	var task Task
	if err := task.Scan(row); err != nil {
		return err
	}
	l.Body = append(l.Body, task)
	return nil
}

// QueueStatus
func (s *QueueStatus) Scan(row pg.Row) error {
	return row.Scan(&s.Queue, &s.Status, &s.Count)
}

// QueueStatusResponse
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
	// Set queue name
	if name, err := q.queueName(); err != nil {
		return "", err
	} else {
		bind.Set("id", name)
	}

	switch op {
	case pg.Get:
		return bind.Replace("${pgqueue.get}"), nil
	case pg.Update:
		return bind.Replace("${pgqueue.patch}"), nil
	case pg.Delete:
		return bind.Replace("${pgqueue.delete}"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("Unsupported QueueName operation %q", op)
	}
}

func (q QueueCleanRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Set queue name
	if name, err := QueueName(q.Queue).queueName(); err != nil {
		return "", err
	} else {
		bind.Set("id", name)
	}

	switch op {
	case pg.List:
		return bind.Replace("${pgqueue.clean}"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("Unsupported QueueCleanRequest operation %q", op)
	}
}

func (l QueueListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	bind.Set("where", "")
	l.OffsetLimit.Bind(bind, QueueListLimit)

	switch op {
	case pg.List:
		return bind.Replace("${pgqueue.list}"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("Unsupported QueueListRequest operation %q", op)
	}
}

func (l QueueStatusRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	var where []string

	// Bind parameters
	if bind.Has("id") {
		where = append(where, `q."queue" = @id`)
	}

	// Where clause - note: query already has "WHERE q.ns = @ns", so we use AND
	if len(where) > 0 {
		bind.Set("where", "AND "+strings.Join(where, " AND "))
	} else {
		bind.Set("where", "")
	}

	switch op {
	case pg.List:
		return bind.Replace("${pgqueue.stats}"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("Unsupported QueueStatusRequest operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

// Insert
func (q QueueMeta) Insert(bind *pg.Bind) (string, error) {
	// Queue name
	queue, err := QueueName(q.Queue).queueName()
	if err != nil {
		return "", err
	} else {
		bind.Set("queue", queue)
	}

	// Note: Inserts default values for ttl, retries, retry_delay
	// A subsequent update is required to set these values

	// Return the insert query
	return bind.Replace("${pgqueue.insert}"), nil
}

// Patch
func (q QueueMeta) Update(bind *pg.Bind) error {
	var patch []string

	// Queue name
	if q.Queue != "" {
		if queue, err := QueueName(q.Queue).queueName(); err != nil {
			return err
		} else {
			patch = append(patch, `queue=`+bind.Set("queue", queue))
		}
	}

	// Set patch values
	if q.TTL != nil {
		patch = append(patch, `ttl=`+bind.Set("ttl", q.TTL))
	}
	if q.Retries != nil {
		patch = append(patch, `retries=`+bind.Set("retries", q.Retries))
	}
	if q.RetryDelay != nil {
		patch = append(patch, `retry_delay=`+bind.Set("retry_delay", q.RetryDelay))
	}

	// Check patch values
	if len(patch) == 0 {
		return httpresponse.ErrBadRequest.With("No patch values")
	} else {
		bind.Set("patch", strings.Join(patch, ", "))
	}

	// Return success
	return nil
}

// Normalize queue name
func (q QueueName) queueName() (string, error) {
	if queue := strings.ToLower(strings.TrimSpace(string(q))); queue == "" {
		return "", httpresponse.ErrBadRequest.With("Missing queue name")
	} else if !types.IsIdentifier(queue) {
		return "", httpresponse.ErrBadRequest.Withf("Invalid queue name: %q", queue)
	} else {
		return queue, nil
	}
}
