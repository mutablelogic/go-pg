package manager

import (
	"os"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	metric "go.opentelemetry.io/otel/metric"
	trace "go.opentelemetry.io/otel/trace"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Opt configures a Manager during construction.
type Opt func(*opt) error

// opt combines all configuration options for Manager.
type opt struct {
	worker  string
	schema  string
	tracer  trace.Tracer
	metrics metric.Meter
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func (o *opt) apply(opts ...Opt) error {
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(o); err != nil {
			return err
		}
	}
	return nil
}

func (o *opt) defaults() error {
	o.schema = schema.DefaultSchema
	if hostname, err := os.Hostname(); err != nil {
		return err
	} else {
		o.worker = hostname
	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WithSchema sets the database schema names to use for all queries. If not set the default schemas are used.
func WithSchema(schemaName string) Opt {
	return func(o *opt) error {
		if schemaName != "" {
			o.schema = schemaName
		}
		return nil
	}
}

// WithWorker sets the worker name used for manager tasks. If not set the hostname is used.
func WithWorker(workerName string) Opt {
	return func(o *opt) error {
		if workerName != "" {
			o.worker = workerName
		}
		return nil
	}
}

// WithTracer sets the OpenTelemetry tracer used for manager spans.
func WithTracer(tracer trace.Tracer) Opt {
	return func(o *opt) error {
		o.tracer = tracer
		return nil
	}
}

// WithMeter sets the OpenTelemetry meter used for manager metrics.
func WithMeter(meter metric.Meter) Opt {
	return func(o *opt) error {
		o.metrics = meter
		return nil
	}
}
