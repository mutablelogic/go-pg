package broadcaster

import (
	// Packages
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgbroadcaster/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Opt is a functional option for the broadcaster manager's configuration
type Opt func(*opt) error

type opt struct {
	tracer  trace.Tracer
	schema  string
	channel string
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func (o *opt) apply(opt []Opt) error {
	o.defaults()

	// Apply options
	for _, fn := range opt {
		if err := fn(o); err != nil {
			return err
		}
	}

	// Return success
	return nil
}

func (o *opt) defaults() {
	o.tracer = nil
	o.schema = schema.DefaultSchema
	o.channel = schema.NotifyChannel
}

////////////////////////////////////////////////////////////////////////////////
// OPTIONS

// WithTracer sets the tracer used for tracing operations
func WithTracer(tracer trace.Tracer) Opt {
	return func(o *opt) error {
		o.tracer = tracer
		return nil
	}
}

// WithSchema sets the database schema, which must be an identifier
func WithSchema(name string) Opt {
	return func(o *opt) error {
		if !types.IsIdentifier(name) {
			return pg.ErrBadParameter.Withf("invalid schema: %q", name)
		}
		o.schema = name
		return nil
	}
}

// WithChannel sets the channel the notifications are listened for on, which
// must be an identifier
func WithChannel(channel string) Opt {
	return func(o *opt) error {
		if !types.IsIdentifier(channel) {
			return pg.ErrBadParameter.Withf("invalid channel: %q", channel)
		}
		o.channel = channel
		return nil
	}
}
