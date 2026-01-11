package queue

import (
	"context"

	// Packages
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ListNamespaces returns all distinct namespaces from the queue table
func (manager *Manager) ListNamespaces(ctx context.Context, req schema.NamespaceListRequest) (*schema.NamespaceList, error) {
	var list schema.NamespaceList
	if err := manager.conn.List(ctx, &list, req); err != nil {
		return nil, err
	}
	return &list, nil
}
