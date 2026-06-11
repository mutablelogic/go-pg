package httpclient

import (
	"testing"

	// Packages
	assert "github.com/stretchr/testify/assert"
)

func Test_WithOffsetLimit(t *testing.T) {
	assert := assert.New(t)

	t.Run("ZeroOffsetNoLimit", func(t *testing.T) {
		opt, err := applyOpts(WithOffsetLimit(0, nil))
		assert.NoError(err)
		assert.NotNil(opt)
		assert.Empty(opt.Values)
	})

	t.Run("WithOffset", func(t *testing.T) {
		opt, err := applyOpts(WithOffsetLimit(10, nil))
		assert.NoError(err)
		assert.NotNil(opt)
		assert.Equal("10", opt.Get("offset"))
		assert.Empty(opt.Get("limit"))
	})

	t.Run("WithLimit", func(t *testing.T) {
		limit := uint64(20)
		opt, err := applyOpts(WithOffsetLimit(0, &limit))
		assert.NoError(err)
		assert.NotNil(opt)
		assert.Empty(opt.Get("offset"))
		assert.Equal("20", opt.Get("limit"))
	})

	t.Run("WithOffsetAndLimit", func(t *testing.T) {
		limit := uint64(50)
		opt, err := applyOpts(WithOffsetLimit(100, &limit))
		assert.NoError(err)
		assert.NotNil(opt)
		assert.Equal("100", opt.Get("offset"))
		assert.Equal("50", opt.Get("limit"))
	})
}

func Test_WithQueue(t *testing.T) {
	assert := assert.New(t)

	t.Run("EmptyQueue", func(t *testing.T) {
		opt, err := applyOpts(WithQueue(""))
		assert.NoError(err)
		assert.NotNil(opt)
		assert.Empty(opt.Get("queue"))
	})

	t.Run("ValidQueue", func(t *testing.T) {
		opt, err := applyOpts(WithQueue("test-queue"))
		assert.NoError(err)
		assert.NotNil(opt)
		assert.Equal("test-queue", opt.Get("queue"))
	})

	t.Run("QueueWithSpecialChars", func(t *testing.T) {
		opt, err := applyOpts(WithQueue("queue_with-special.chars"))
		assert.NoError(err)
		assert.NotNil(opt)
		assert.Equal("queue_with-special.chars", opt.Get("queue"))
	})
}

func Test_WithWorker(t *testing.T) {
	assert := assert.New(t)

	t.Run("EmptyWorker", func(t *testing.T) {
		opt, err := applyOpts(WithWorker(""))
		assert.NoError(err)
		assert.NotNil(opt)
		assert.Empty(opt.Get("worker"))
	})

	t.Run("ValidWorker", func(t *testing.T) {
		opt, err := applyOpts(WithWorker("worker-001"))
		assert.NoError(err)
		assert.NotNil(opt)
		assert.Equal("worker-001", opt.Get("worker"))
	})

	t.Run("WorkerWithUUID", func(t *testing.T) {
		workerID := "550e8400-e29b-41d4-a716-446655440000"
		opt, err := applyOpts(WithWorker(workerID))
		assert.NoError(err)
		assert.NotNil(opt)
		assert.Equal(workerID, opt.Get("worker"))
	})
}

func Test_OptSet(t *testing.T) {
	assert := assert.New(t)

	t.Run("EmptyValue", func(t *testing.T) {
		opt, err := applyOpts(OptSet("key", ""))
		assert.NoError(err)
		assert.NotNil(opt)
		assert.Empty(opt.Get("key"))
	})

	t.Run("ValidKeyValue", func(t *testing.T) {
		opt, err := applyOpts(OptSet("custom_param", "custom_value"))
		assert.NoError(err)
		assert.NotNil(opt)
		assert.Equal("custom_value", opt.Get("custom_param"))
	})

	t.Run("MultipleValues", func(t *testing.T) {
		opt, err := applyOpts(
			OptSet("param1", "value1"),
			OptSet("param2", "value2"),
		)
		assert.NoError(err)
		assert.NotNil(opt)
		assert.Equal("value1", opt.Get("param1"))
		assert.Equal("value2", opt.Get("param2"))
	})
}

func Test_ApplyOpts_Multiple(t *testing.T) {
	assert := assert.New(t)

	t.Run("CombinedOptions", func(t *testing.T) {
		limit := uint64(25)
		opt, err := applyOpts(
			WithOffsetLimit(10, &limit),
			WithQueue("my-queue"),
			WithWorker("worker-123"),
			OptSet("status", "pending"),
		)
		assert.NoError(err)
		assert.NotNil(opt)
		assert.Equal("10", opt.Get("offset"))
		assert.Equal("25", opt.Get("limit"))
		assert.Equal("my-queue", opt.Get("queue"))
		assert.Equal("worker-123", opt.Get("worker"))
		assert.Equal("pending", opt.Get("status"))
	})

	t.Run("NoOptions", func(t *testing.T) {
		opt, err := applyOpts()
		assert.NoError(err)
		assert.NotNil(opt)
		assert.Empty(opt.Values)
	})

	t.Run("OverwriteValue", func(t *testing.T) {
		opt, err := applyOpts(
			OptSet("key", "value1"),
			OptSet("key", "value2"),
		)
		assert.NoError(err)
		assert.NotNil(opt)
		// The second OptSet should overwrite the first
		assert.Equal("value2", opt.Get("key"))
	})
}
