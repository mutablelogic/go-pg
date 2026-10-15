package httpclient_test

import (
	"context"
	"encoding/json"
	"testing"

	// Packages
	pgqueue "github.com/mutablelogic/go-pg/pgqueue"
	httpclient "github.com/mutablelogic/go-pg/pgqueue/httpclient"
	test "github.com/mutablelogic/go-pg/pgqueue/test"
	require "github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	test.Main(m, nil)
}

// newClient returns an isolated manager, and a client for a test server
// serving its API
func newClient(t *testing.T) (*pgqueue.Manager, *httpclient.Client, context.Context) {
	t.Helper()
	_, ctx := test.Begin(t)
	t.Cleanup(func() { test.End(t) })
	manager := test.IsolatedManager(t)
	server := test.NewServer(t, manager)
	client, err := httpclient.New(server.URL + test.Prefix)
	require.NoError(t, err)
	return manager, client, ctx
}

// noopTask is a task callback which does nothing
func noopTask(context.Context, json.RawMessage) (any, error) {
	return nil, nil
}
