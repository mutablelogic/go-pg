// Copyright 2026 David Thorpe
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package manager

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgqueue/schema"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func TestRegisterTask(t *testing.T) {
	registry := NewExec(nil)
	fn := func(context.Context, json.RawMessage) (any, error) { return nil, nil }

	err := registry.RegisterTask("  Example_Task  ", fn)
	require.NoError(t, err)
	if assert.NotNil(t, registry.t) {
		_, exists := registry.t["example_task"]
		assert.True(t, exists)
	}

	err = registry.RegisterTask("example_task", fn)
	require.Error(t, err)
	assert.True(t, errors.Is(err, httpresponse.ErrConflict))
}

func TestRemoveTask(t *testing.T) {
	registry := NewExec(nil)
	fn := func(context.Context, json.RawMessage) (any, error) { return nil, nil }

	require.NoError(t, registry.RegisterTask("remove_task", fn))
	require.NoError(t, registry.RemoveTask("remove_task"))

	err := registry.RemoveTask("remove_task")
	require.Error(t, err)
	assert.True(t, errors.Is(err, httpresponse.ErrNotFound))
}

func TestRunRecoversPanic(t *testing.T) {
	result := run(context.Background(), func(context.Context, json.RawMessage) (any, error) {
		panic("boom")
	}, nil)

	require.NotNil(t, result)
	require.Error(t, result.Error)
	assert.EqualError(t, result.Error, "panic: boom")
	assert.Nil(t, result.Result)
}

func TestRunTickerTaskKeepsContextAlive(t *testing.T) {
	exec := NewExec(nil)
	results := make(chan *Result, 1)
	ticker := &schema.Ticker{Ticker: "alive_context"}

	require.NoError(t, exec.RegisterTask(ticker.Ticker, func(ctx context.Context, _ json.RawMessage) (any, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return map[string]bool{"ok": true}, nil
		}
	}))

	require.NoError(t, exec.RunTickerTask(context.Background(), ticker, results))

	select {
	case result := <-results:
		require.NotNil(t, result)
		require.NoError(t, result.Error)
		assert.Equal(t, ticker.Ticker, result.Ticker)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ticker result")
	}

	exec.Close()
}

func TestRunQueueTaskUsesTaskTTLDeadline(t *testing.T) {
	exec := NewExec(nil)
	results := make(chan *Result, 1)
	diesAt := time.Now().Add(2 * time.Second)
	task := &schema.Task{Id: 42, Queue: "queue_deadline", DiesAt: diesAt}

	require.NoError(t, exec.RegisterTask(task.Queue, func(ctx context.Context, _ json.RawMessage) (any, error) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		assert.WithinDuration(t, diesAt.UTC(), deadline.UTC(), 50*time.Millisecond)
		return map[string]bool{"ok": true}, nil
	}))

	exec.RunQueueTask(context.Background(), task, results)

	select {
	case result := <-results:
		require.NotNil(t, result)
		require.NoError(t, result.Error)
		assert.Equal(t, task.Queue, result.Queue)
		if assert.NotNil(t, result.Task) {
			assert.Equal(t, task.Id, result.Task.Id)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queue task result")
	}

	exec.Close()
}

func TestRunQueueTaskRequiresTaskDeadline(t *testing.T) {
	exec := NewExec(nil)
	results := make(chan *Result, 1)
	task := &schema.Task{Id: 99, Queue: "queue_deadline_required"}
	called := false

	require.NoError(t, exec.RegisterTask(task.Queue, func(ctx context.Context, _ json.RawMessage) (any, error) {
		called = true
		return nil, nil
	}))

	exec.RunQueueTask(context.Background(), task, results)
	assert.False(t, called)

	select {
	case result := <-results:
		require.NotNil(t, result)
		require.Error(t, result.Error)
		assert.True(t, errors.Is(result.Error, httpresponse.ErrBadRequest))
		assert.Equal(t, task.Queue, result.Queue)
		if assert.NotNil(t, result.Task) {
			assert.Equal(t, task.Id, result.Task.Id)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queue task error result")
	}

	exec.Close()
}
