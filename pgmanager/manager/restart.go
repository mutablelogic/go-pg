package manager

import (
	"bytes"
	"context"
	"os/exec"
	"strings"
	"time"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// How long the server has to start again, once it's asked to restart,
	// and how often it's checked
	restartTimeout  = 2 * time.Minute
	restartInterval = 500 * time.Millisecond
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Restart restarts the server: it runs the command which asks the server's
// supervisor to restart it, such as s6-svc -r, which ends every session,
// and waits until the server has started again, which is when it started
// later than it did. It's not available unless there's a command, with
// WithRestart.
func (manager *Manager) Restart(ctx context.Context) (_ *schema.Restart, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "Restart")
	defer func() { endSpan(err) }()

	if len(manager.restart) == 0 {
		return nil, pg.ErrNotAvailable.With("restarting the server isn't turned on, with --pg.restart")
	}

	// When the server started, before it's restarted
	var before schema.Postmaster
	if err := manager.conn.Get(ctx, &before, before); err != nil {
		return nil, err
	}

	// Ask the supervisor to restart it
	var output bytes.Buffer
	cmd := exec.CommandContext(ctx, manager.restart[0], manager.restart[1:]...)
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Run(); err != nil {
		if out := strings.TrimSpace(output.String()); out != "" {
			return nil, pg.ErrNotAvailable.Withf("the server couldn't be restarted: %v: %s", err, out)
		}
		return nil, pg.ErrNotAvailable.Withf("the server couldn't be restarted: %v", err)
	}

	// Wait for it to start again
	ctx, cancel := context.WithTimeout(ctx, restartTimeout)
	defer cancel()
	started, err := waitForStart(ctx, before.Started, restartInterval, func(ctx context.Context) (time.Time, error) {
		var now schema.Postmaster
		err := manager.conn.Get(ctx, &now, now)
		return now.Started, err
	})
	if err != nil {
		return nil, pg.ErrNotAvailable.Withf("the server was asked to restart, and hasn't started again, in %v", restartTimeout)
	}
	return &schema.Restart{Started: started}, nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// waitForStart waits until the server started later than it did before,
// checking every interval, as it fails while it's down, and returns when it
// started, or the context's error, when it's done first
func waitForStart(ctx context.Context, before time.Time, interval time.Duration, started func(context.Context) (time.Time, error)) (time.Time, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-ticker.C:
			if now, err := started(ctx); err == nil && now.After(before) {
				return now, nil
			}
		}
	}
}
