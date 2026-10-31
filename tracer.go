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
// PUBLIC METHODS

func (t *tracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	qd := &queryData{
		sql:  data.SQL,
		args: data.Args,
	}

	// Start OTEL span if tracer is configured
	if t.otel != nil {
		ctx, qd.span = t.otel.Start(ctx, spanName(qd.sql, qd.args),
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
		t.TraceFn(ctx, strings.TrimSpace(qd.sql), args(qd.sql, qd.args), data.Err)
	}
}

//////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// spanName returns the name of the span of a query: the query's name, when it
// has one (see Bind.Query), or else pg.transaction, pg.commit or pg.rollback
// for the statements of a transaction, including those of a savepoint, which
// is how pgx nests transactions, or else pg.query
func spanName(sql string, args []any) string {
	if args, ok := namedArgs(args); ok {
		if name, ok := args[TraceSpanNameArg].(string); ok && name != "" {
			return name
		}
	}

	// The first two words of the statement, in lower case
	words := strings.Fields(strings.ToLower(sql))
	if len(words) > 2 {
		words = words[:2]
	}
	switch {
	case len(words) == 0:
		return "pg.query"
	case words[0] == "begin", words[0] == "savepoint", words[0] == "start" && len(words) > 1 && words[1] == "transaction":
		return "pg.transaction"
	case words[0] == "commit", words[0] == "end", words[0] == "release":
		return "pg.commit"
	case words[0] == "rollback", words[0] == "abort":
		return "pg.rollback"
	default:
		return "pg.query"
	}
}

// args normalizes traced query arguments and filters named arguments down to
// the @bind placeholders referenced by the traced SQL.
func args(query string, args []any) any {
	if len(args) == 0 {
		return nil
	}
	if args, ok := namedArgs(args); ok {
		return filterNamedArgs(query, args)
	}
	return args
}

// namedArgs unwraps the pgx named-argument forms used by this package.
func namedArgs(args []any) (pgx.NamedArgs, bool) {
	if len(args) != 1 {
		return nil, false
	}
	switch value := args[0].(type) {
	case pgx.NamedArgs:
		return value, true
	case map[string]any:
		result := make(pgx.NamedArgs, len(value))
		for key, value := range value {
			result[key] = value
		}
		return result, true
	default:
		return nil, false
	}
}

// filterNamedArgs returns only the named arguments referenced by @bind
// placeholders in the traced SQL.
func filterNamedArgs(query string, args pgx.NamedArgs) pgx.NamedArgs {
	keys := queryArgs(query)
	if len(keys) == 0 {
		return nil
	}
	filtered := make(pgx.NamedArgs, len(keys))
	for key := range keys {
		if value, ok := args[key]; ok {
			filtered[key] = value
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

// queryArgs collects placeholder names from @bind parameters in the traced
// SQL passed to pgx.
func queryArgs(query string) map[string]struct{} {
	keys := make(map[string]struct{})
	for i := 0; i < len(query); i++ {
		if query[i] != '@' || i+1 >= len(query) || !isArgStart(query[i+1]) {
			continue
		}
		j := i + 2
		for j < len(query) && isArgPart(query[j]) {
			j++
		}
		keys[query[i+1:j]] = struct{}{}
		i = j - 1
	}
	return keys
}

func isArgStart(ch byte) bool {
	return ch == '_' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z'
}

func isArgPart(ch byte) bool {
	return isArgStart(ch) || ch >= '0' && ch <= '9'
}
