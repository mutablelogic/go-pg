// Package broadcaster listens for the database's notifications, on a channel,
// so the server's modules, and its clients, can be told of changes, with one
// connection which listens, rather than one each.
package broadcaster

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgbroadcaster/schema"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	opt
	pg.PoolConn

	// Whether the manager is running, and whether it's listening on its
	// channel
	running   atomic.Bool
	listening atomic.Bool

	// The channels which are sent the events, by Subscribe, whether the
	// manager has stopped running, so later subscriptions end at once, and
	// the channel which is closed once it's listening on its channel
	mu          sync.Mutex
	subscribers map[chan<- schema.Event]*subscriber
	stopped     bool
	ready       chan struct{}
}

// subscriber is a subscription's filter, and the channel which is closed
// when the manager stops running, which ends the subscription
type subscriber struct {
	filter schema.EventFilter
	stop   chan struct{}
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a new manager, which listens for the notifications on its
// channel when it's run
func New(ctx context.Context, pool pg.PoolConn, opts ...Opt) (_ *Manager, err error) {
	self := &Manager{subscribers: make(map[chan<- schema.Event]*subscriber), ready: make(chan struct{})}

	// Check parameters
	if pool == nil {
		return nil, pg.ErrBadParameter.With("pg pool is required")
	}

	// Apply options
	if err := self.opt.apply(opts); err != nil {
		return nil, err
	}

	// Trace creating the manager, which needs the tracer from the options
	_, endSpan := otel.StartSpan(self.tracer, ctx, "pgbroadcaster.New",
		attribute.String("arg.schema", self.schema),
		attribute.String("arg.channel", self.channel),
	)
	defer func() { endSpan(err) }()

	// Parse and register named queries, and bind the schema
	queries, err := pg.NewQueries(strings.NewReader(schema.Queries))
	if err != nil {
		return nil, pg.ErrInternalServerError.Withf("queries.sql: %w", err)
	} else {
		self.PoolConn = pool.WithQueries(queries).With("schema", self.schema).(pg.PoolConn)
	}

	// Return success
	return self, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Schema returns the database schema of the manager
func (self *Manager) Schema() string {
	return self.schema
}

// Channel returns the channel the manager listens for notifications on
func (self *Manager) Channel() string {
	return self.channel
}
