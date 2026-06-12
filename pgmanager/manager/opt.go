package manager

import (
	"errors"
	"strings"

	// Packages
	metric "go.opentelemetry.io/otel/metric"
	trace "go.opentelemetry.io/otel/trace"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Opt configures a Manager during construction.
type Opt func(*opts) error

// combines all configuration options for Manager.
type opts struct {
	tracer  trace.Tracer
	metrics metric.Meter
	cluster string
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func (o *opts) apply(opts ...Opt) error {
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

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WithTracer sets the OpenTelemetry tracer used for manager spans.
func WithTracer(tracer trace.Tracer) Opt {
	return func(o *opts) error {
		o.tracer = tracer
		return nil
	}
}

// WithMeter sets the OpenTelemetry meter used for manager metrics.
func WithMeter(meter metric.Meter) Opt {
	return func(o *opts) error {
		o.metrics = meter
		return nil
	}
}

// WithClusterName sets the name to be used as an attribute on all metrics and spans.
func WithClusterName(cluster string) Opt {
	return func(o *opts) error {
		if cluster := strings.TrimSpace(cluster); cluster != "" {
			o.cluster = cluster
		} else {
			return errors.New("cluster name cannot be empty")
		}
		return nil
	}
}
