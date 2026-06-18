package manager

import (
	"os"
	"time"

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
	worker             string
	schema             string
	tracer             trace.Tracer
	metrics            metric.Meter
	partitionSize      uint64
	partitionThreshold float64
	partitionAhead     uint64
	maintenancePeriod  time.Duration
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
	o.partitionSize = schema.DefaultPartitionSize
	o.partitionThreshold = schema.DefaultPartitionThreshold
	o.partitionAhead = schema.DefaultPartitionAhead
	o.maintenancePeriod = schema.DefaultMaintenancePeriod
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

// WithPartitionSize sets the task partition size. Values less than 1 are ignored.
func WithPartitionSize(size uint64) Opt {
	return func(o *opt) error {
		if size > 0 {
			o.partitionSize = size
		}
		return nil
	}
}

// WithPartitionThreshold sets the partition creation threshold in the range (0,1].
func WithPartitionThreshold(threshold float64) Opt {
	return func(o *opt) error {
		if threshold > 0 && threshold <= 1 {
			o.partitionThreshold = threshold
		}
		return nil
	}
}

// WithPartitionAhead sets how many partitions to create when threshold is reached.
func WithPartitionAhead(ahead uint64) Opt {
	return func(o *opt) error {
		if ahead > 0 {
			o.partitionAhead = ahead
		}
		return nil
	}
}

// WithMaintenancePeriod sets how often the maintenance ticker runs. Values less than 1 second are ignored.
func WithMaintenancePeriod(period time.Duration) Opt {
	return func(o *opt) error {
		if period >= time.Second {
			o.maintenancePeriod = period
		}
		return nil
	}
}
