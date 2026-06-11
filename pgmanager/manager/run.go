package manager

import (
	"context"
)

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// Run the manager - currently this just waits for the context to be cancelled,
// but in the future it will run background tasks
func (manager *Manager) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}
