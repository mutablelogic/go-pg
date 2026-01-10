package queue_test

import (
	"context"
	"testing"

	// Packages
	queue "github.com/mutablelogic/go-pg/pkg/queue"
	schema "github.com/mutablelogic/go-pg/pkg/queue/schema"
	assert "github.com/stretchr/testify/assert"
)

////////////////////////////////////////////////////////////////////////////////
// NAMESPACE TESTS

func Test_Namespace_ListNamespaces(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()
	ctx := context.TODO()

	t.Run("EmptyList", func(t *testing.T) {
		// Create manager with unique namespace
		mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_ns_empty"))
		assert.NoError(err)
		assert.NotNil(mgr)

		// List namespaces - should include at least the system namespace
		list, err := mgr.ListNamespaces(ctx, schema.NamespaceListRequest{})
		assert.NoError(err)
		assert.NotNil(list)
		// The pgqueue system namespace is always created
		assert.Contains(list.Body, schema.SchemaName)
	})

	t.Run("WithQueues", func(t *testing.T) {
		// Create manager with unique namespace
		mgr, err := queue.New(ctx, conn, queue.WithNamespace("test_ns_queues"))
		assert.NoError(err)
		assert.NotNil(mgr)

		// Create a queue to ensure the namespace exists in the queue table
		_, err = mgr.RegisterQueue(ctx, schema.QueueMeta{
			Queue: "test-queue",
		})
		assert.NoError(err)

		// List namespaces - should include our namespace
		list, err := mgr.ListNamespaces(ctx, schema.NamespaceListRequest{})
		assert.NoError(err)
		assert.NotNil(list)
		assert.Contains(list.Body, "test_ns_queues")
	})

	t.Run("MultipleNamespaces", func(t *testing.T) {
		// Create multiple managers with different namespaces
		mgr1, err := queue.New(ctx, conn, queue.WithNamespace("test_ns_multi1"))
		assert.NoError(err)
		_, err = mgr1.RegisterQueue(ctx, schema.QueueMeta{Queue: "q1"})
		assert.NoError(err)

		mgr2, err := queue.New(ctx, conn, queue.WithNamespace("test_ns_multi2"))
		assert.NoError(err)
		_, err = mgr2.RegisterQueue(ctx, schema.QueueMeta{Queue: "q2"})
		assert.NoError(err)

		// List namespaces from either manager should show both
		list, err := mgr1.ListNamespaces(ctx, schema.NamespaceListRequest{})
		assert.NoError(err)
		assert.Contains(list.Body, "test_ns_multi1")
		assert.Contains(list.Body, "test_ns_multi2")
	})
}
