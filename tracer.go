package pg

import (
	"context"
	"strings"

	// Packages
	pgx "github.com/jackc/pgx/v5"
	attribute "go.opentelemetry.io/otel/attribute"
	codes "go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	trace "go.opentelemetry.io/otel/trace"
)

//////////////////////////////////////////////////////////////////////////////
// TYPES

// Tracer is a postgresql query tracer. It is safe for concurrent use.
type tracer struct {
	TraceFn
	otel trace.Tracer
}

// queryData holds per-query tracing data stored in context
type queryData struct {
	span trace.Span
	sql  string
	args []any
}

// ctxKey is the context key for query data
type ctxKey struct{}

// TraceFn is a function which is called when a query is executed,
// with the execution context, the SQL and arguments, and the error
// if any was generated
type TraceFn func(context.Context, string, any, error)

//////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	TraceSpanNameArg = "otelspan"
)

//////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewTracer creates a new query tracer with an optional callback function.
func NewTracer(fn TraceFn) *tracer {
	return &tracer{
		TraceFn: fn,
	}
}

// NewOTELTracer creates a new query tracer that emits OpenTelemetry spans.
// Each query will create a new span. If fn is non-nil, it will also be called on query end.
func NewOTELTracer(t trace.Tracer) *tracer {
	return &tracer{
		otel: t,
	}
}

//////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (t *tracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	qd := &queryData{
		sql:  data.SQL,
		args: data.Args,
	}

	otelSpanName := func() string {
		if args_, ok := args(qd.args).(pgx.NamedArgs); ok {
			if v, ok := args_[TraceSpanNameArg]; ok {
				if s, ok := v.(string); ok && s != "" {
					return s
				}
			}
		}
		return "pg.query"
	}

	// Start OTEL span if tracer is configured
	if t.otel != nil {
		ctx, qd.span = t.otel.Start(ctx, otelSpanName(),
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(
				semconv.DBSystemPostgreSQL,
				attribute.String("db.statement", data.SQL),
			),
		)
	}

	return context.WithValue(ctx, ctxKey{}, qd)
}

func (t *tracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	qd, ok := ctx.Value(ctxKey{}).(*queryData)
	if !ok {
		return
	}

	// End OTEL span if present
	if qd.span != nil {
		if data.Err != nil {
			qd.span.RecordError(data.Err)
			qd.span.SetStatus(codes.Error, data.Err.Error())
		}
		qd.span.End()
	}

	// Call TraceFn if configured
	if t.TraceFn != nil {
		t.TraceFn(ctx, strings.TrimSpace(qd.sql), args(qd.args), data.Err)
	}
}

//////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func args(args []any) any {
	if len(args) == 0 {
		return nil
	}
	if len(args) == 1 {
		return args[0]
	}
	return args
}
