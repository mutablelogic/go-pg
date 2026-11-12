package broadcaster

import (
	"context"
	"encoding/json"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgbroadcaster/schema"
	changeschema "github.com/mutablelogic/go-server/broadcaster/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
	attribute "go.opentelemetry.io/otel/attribute"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Watch sends the changes which are published to the broadcaster, and match
// the filter, to the channel, as the broadcaster's Watch does, until the
// context is cancelled, or the broadcaster stops running
func (self *Manager) Watch(ctx context.Context, filter changeschema.ChangeFilter, ch chan<- changeschema.Change) (<-chan error, error) {
	return self.broadcaster.Watch(ctx, filter, ch)
}

// Publish publishes the change to the manager's channel, as JSON, so every
// server listening on it publishes it to its broadcaster. Returns
// ErrBadParameter when the change is larger than a notification can be.
func (self *Manager) Publish(ctx context.Context, change changeschema.Change) (err error) {
	ctx, endSpan := otel.StartSpan(self.tracer, ctx, "pgbroadcaster.Publish",
		attribute.String("arg.channel", self.channel),
		attribute.String("arg.change", types.Stringify(change)),
	)
	defer func() { endSpan(err) }()

	payload, err := json.Marshal(change)
	if err != nil {
		return err
	} else if len(payload) > schema.MaxPayloadSize {
		return pg.ErrBadParameter.Withf("change is %d bytes, which is more than %d", len(payload), schema.MaxPayloadSize)
	}
	return self.PoolConn.With("channel", self.channel, "payload", string(payload)).Exec(ctx, "${pgbroadcaster.publish}")
}
