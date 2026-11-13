package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	// Packages
	assert "github.com/stretchr/testify/assert"
)

func Test_waitForStart(t *testing.T) {
	before := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	later := before.Add(time.Minute)

	// started returns, from each call, the next of the times, or errors,
	// and then the last, again
	started := func(results ...any) func(context.Context) (time.Time, error) {
		i := 0
		return func(context.Context) (time.Time, error) {
			r := results[min(i, len(results)-1)]
			i++
			if err, ok := r.(error); ok {
				return time.Time{}, err
			}
			return r.(time.Time), nil
		}
	}
	down := errors.New("the server is down")

	t.Run("a later start", func(t *testing.T) {
		got, err := waitForStart(context.Background(), before, time.Millisecond, started(before, down, down, later))
		assert.NoError(t, err)
		assert.Equal(t, later, got)
	})

	t.Run("an unchanged start times out", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := waitForStart(ctx, before, time.Millisecond, started(before))
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("an earlier start isn't a restart", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := waitForStart(ctx, before, time.Millisecond, started(before.Add(-time.Minute)))
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("a server which stays down times out", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := waitForStart(ctx, before, time.Millisecond, started(down))
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := waitForStart(ctx, before, time.Hour, started(later))
		assert.ErrorIs(t, err, context.Canceled)
	})
}
