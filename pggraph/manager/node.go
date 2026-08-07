package manager

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pggraph/schema"
	queueschema "github.com/mutablelogic/go-pg/pgqueue/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// RegisterNode creates a new node, or updates an existing node, and returns it.
func (manager *Manager) RegisterNode(ctx context.Context, name string, meta schema.NodeMeta, fn any) (_ *schema.Node, err error) {
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "RegisterNode",
		attribute.String("name", name),
		attribute.String("meta", types.Stringify(meta)),
	)
	defer func() { endSpan(err) }()

	// Determine the input and output edges from the function signature
	_, in, out, err := functionEdges(fn)
	if err != nil {
		return nil, err
	}

	// Create the node in the database
	insert := schema.NodeInsert{
		Name:     name,
		In:       out,
		Out:      in,
		NodeMeta: meta,
	}

	var node schema.Node
	if err := manager.queue.Tx(ctx, func(conn pg.Conn) error {
		// Create the node
		if err := conn.With("version", manager.version).Insert(ctx, &node, insert); err != nil {
			return err
		}

		// TODO: Create the task queue for the node
		_, err := manager.queue.RegisterQueue(ctx, name, queueschema.QueueMeta{}, manager.taskRunner(node, fn))
		if err != nil {
			return err
		}

		// Return success
		return nil
	}); err != nil {
		return nil, err
	}

	// Return the node
	return types.Ptr(node), nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func functionEdges(fn any) (string, []schema.Edge, schema.Edge, error) {
	if fn == nil {
		return "", nil, "", pg.ErrBadParameter.With("fn is missing")
	}

	value := reflect.ValueOf(fn)
	if value.Kind() != reflect.Func {
		return "", nil, "", pg.ErrBadParameter.With("fn is not a function")
	}

	fnType := value.Type()
	if fnType.NumIn() < 2 {
		return "", nil, "", pg.ErrBadParameter.With("fn must accept context.Context and at least one input")
	}
	if fnType.In(0) != reflect.TypeOf((*context.Context)(nil)).Elem() {
		return "", nil, "", pg.ErrBadParameter.With("fn must accept context.Context as the first argument")
	}
	if fnType.NumOut() != 2 {
		return "", nil, "", pg.ErrBadParameter.With("fn must return exactly one value and error")
	}
	if fnType.Out(1) != reflect.TypeOf((*error)(nil)).Elem() {
		return "", nil, "", pg.ErrBadParameter.With("fn must return error as the last return value")
	}

	runtimeFn := runtime.FuncForPC(value.Pointer())
	if runtimeFn == nil {
		return "", nil, "", pg.ErrBadParameter.With("fn name is missing")
	}
	fnName := runtimeFn.Name()
	if strings.TrimSpace(fnName) == "" {
		return "", nil, "", pg.ErrBadParameter.With("fn name is missing")
	}

	in := make([]schema.Edge, 0, fnType.NumIn()-1)
	for i := 1; i < fnType.NumIn(); i++ {
		edge := schema.Edge(fnType.In(i).String())
		if strings.TrimSpace(string(edge)) == "" {
			return "", nil, "", fmt.Errorf("fn %q has an empty input type", fnName)
		}
		in = append(in, edge)
	}

	out := schema.Edge(fnType.Out(0).String())
	if strings.TrimSpace(string(out)) == "" {
		return "", nil, "", fmt.Errorf("fn %q has an empty return type", fnName)
	}

	return fnName, in, out, nil
}
