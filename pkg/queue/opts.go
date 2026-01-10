package queue

import (
	"errors"
	"os"
	"runtime"
	"strings"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Opt is a functional option for worker pool, queue, and ticker configuration.
type Opt func(*opts) error

type opts struct {
	ns      string
	name    string
	workers int
	period  time.Duration
	tracer  trace.Tracer
}

////////////////////////////////////////////////////////////////////////////////
// ERRORS

var (
	ErrInvalidNamespace  = errors.New("namespace must not be empty")
	ErrReservedNamespace = errors.New("namespace is reserved for system use")
	ErrInvalidWorkers    = errors.New("workers must be >= 1")
	ErrInvalidPeriod     = errors.New("period must be >= 1ms")
)

////////////////////////////////////////////////////////////////////////////////
// OPTIONS

// WithNamespace sets the namespace used to scope all queue operations.
// The namespace cannot be empty or use the reserved system namespace.
func WithNamespace(name string) Opt {
	return func(o *opts) error {
		if name == "" {
			name = schema.DefaultNamespace
		}
		if o.ns = strings.TrimSpace(name); o.ns == schema.SchemaName {
			return ErrReservedNamespace
		}
		return nil
	}
}

// WithTracer sets the tracer used for tracing operations.
func WithTracer(tracer trace.Tracer) Opt {
	return func(o *opts) error {
		o.tracer = tracer
		return nil
	}
}

// WithWorkerName sets the worker name used to identify this worker instance.
// Defaults to the hostname if not specified.
func WithWorkerName(name string) Opt {
	return func(o *opts) error {
		o.name = name
		return nil
	}
}

// WithWorkers sets the number of concurrent workers for the worker pool.
// The worker pool uses a shared pool of workers to process tasks from any registered queue.
// Returns ErrInvalidWorkers if n < 1.
func WithWorkers(n int) Opt {
	return func(o *opts) error {
		if n < 1 {
			return ErrInvalidWorkers
		}
		o.workers = n
		return nil
	}
}

// WithPeriod sets the polling period for a ticker.
// Returns ErrInvalidPeriod if d < 1ms.
func WithPeriod(d time.Duration) Opt {
	return func(o *opts) error {
		if d < time.Millisecond {
			return ErrInvalidPeriod
		}
		o.period = d
		return nil
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func applyOpts(opt []Opt) (opts, error) {
	// Get hostname
	hostname, err := os.Hostname()
	if err != nil {
		return opts{}, err
	}

	// Set defaults
	o := opts{
		ns:      schema.DefaultNamespace,
		name:    hostname,
		workers: runtime.NumCPU(),
		period:  schema.TickerPeriod,
	}

	// Apply options
	for _, fn := range opt {
		if err := fn(&o); err != nil {
			return opts{}, err
		}
	}

	// Return success
	return o, nil
}
