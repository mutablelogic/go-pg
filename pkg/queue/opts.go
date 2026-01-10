package queue

import (
	"errors"
	"os"
	"runtime"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Opt is a functional option for worker pool, queue, and ticker configuration.
type Opt func(*opts) error

type opts struct {
	name    string
	workers int
	period  time.Duration
}

////////////////////////////////////////////////////////////////////////////////
// ERRORS

var (
	ErrInvalidWorkers = errors.New("workers must be >= 1")
	ErrInvalidPeriod  = errors.New("period must be >= 1ms")
)

////////////////////////////////////////////////////////////////////////////////
// OPTIONS

// WithWorkerName sets the worker name used to identify this worker instance.
// Defaults to the hostname if not specified.
func WithWorkerName(name string) Opt {
	return func(o *opts) error {
		o.name = name
		return nil
	}
}

// WithWorkers sets the number of concurrent workers.
// For NewWorkerPool, this sets the default workers for all queues.
// For RegisterQueue, this overrides the pool default for that specific queue.
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
