package httphandler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	// Packages
	httphandler "github.com/mutablelogic/go-pg/pgqueue/httphandler"
	test "github.com/mutablelogic/go-pg/pgqueue/test"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	test.Main(m, nil)
}

func TestRegisterHandlers(t *testing.T) {
	manager, _ := test.Begin(t)
	defer test.End(t)

	router, err := httprouter.NewRouter(context.Background(), http.NewServeMux(), "/api", "*", "pgqueue", "test")
	require.NoError(t, err)
	require.NoError(t, httphandler.RegisterHandlers(manager, router))

	// The tags are documented, and grouped under the task queue
	spec := router.Spec()
	names := make([]string, 0, len(spec.Tags))
	for _, tag := range spec.Tags {
		names = append(names, tag.Name)
		assert.NotEmpty(t, tag.Description, tag.Name)
	}
	assert.ElementsMatch(t, []string{"Queue", "Ticker", "Task"}, names)
	require.Len(t, spec.TagGroups, 1)
	assert.Equal(t, "Task Queue", spec.TagGroups[0].Name)
	assert.Equal(t, []string{"Queue", "Ticker", "Task"}, spec.TagGroups[0].Tags)
}

// noopTask is a task callback which does nothing
func noopTask(context.Context, json.RawMessage) (any, error) {
	return nil, nil
}
