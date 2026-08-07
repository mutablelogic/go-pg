package manager

import (
	"context"
	"log/slog"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (manager *Manager) Run(ctx context.Context, log *slog.Logger) error {
	<-ctx.Done()
	return nil
}
