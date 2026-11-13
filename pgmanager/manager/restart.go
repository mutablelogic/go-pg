package manager

import (
	"context"
	"os"
	"path/filepath"
	"time"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// How long the server has to start again, once it's shut down, and how
	// often it's checked
	restartTimeout  = 2 * time.Minute
	restartInterval = 500 * time.Millisecond
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Restart restarts the server: it signals the server's postmaster to shut
// down fast, which rolls back open transactions, and ends every session,
// and a supervisor, such as docker-postgres's, starts it again, which it
// waits for. The postmaster is found by the postmaster.pid in the server's
// data directory, which is checked to be the server's, by its data
// directory, and when it started, so a file left by another isn't
// signalled. It's not available unless it's turned on, with WithRestart.
func (manager *Manager) Restart(ctx context.Context) (_ *schema.Restart, err error) {
	// Otel span
	ctx, endSpan := otel.StartSpan(manager.tracer, ctx, "Restart")
	defer func() { endSpan(err) }()

	if !manager.restart {
		return nil, pg.ErrNotAvailable.With("restarting the server isn't turned on, with --pg.restart")
	}

	// The server, and its postmaster.pid, which must be its own
	var server schema.Postmaster
	if err := manager.conn.Get(ctx, &server, server); err != nil {
		return nil, err
	}
	path := filepath.Join(server.DataDirectory, schema.PostmasterPIDFile)
	file, err := os.Open(path)
	if err != nil {
		return nil, pg.ErrNotAvailable.Withf("the server's process can't be found: %v", err)
	}
	pid, err := schema.ReadPostmasterPID(file)
	file.Close()
	if err != nil {
		return nil, pg.ErrNotAvailable.Withf("the server's process can't be found: %v", err)
	} else if err := pid.Matches(server); err != nil {
		return nil, pg.ErrNotAvailable.Withf("the server's process can't be found: %v", err)
	}

	// Shut it down fast
	process, err := os.FindProcess(pid.Pid)
	if err != nil {
		return nil, pg.ErrNotAvailable.Withf("the server's process can't be found: %v", err)
	} else if err := process.Signal(os.Interrupt); err != nil {
		return nil, pg.ErrNotAvailable.Withf("the server's process can't be signalled: %v", err)
	}

	// Wait for it to start again, which is when it's started later than it
	// did, as connections fail while it's down
	ctx, cancel := context.WithTimeout(ctx, restartTimeout)
	defer cancel()
	ticker := time.NewTicker(restartInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, pg.ErrNotAvailable.Withf("the server was shut down, and hasn't started again, in %v: is it supervised?", restartTimeout)
		case <-ticker.C:
			var now schema.Postmaster
			if err := manager.conn.Get(ctx, &now, now); err == nil && now.Started.After(server.Started) {
				return &schema.Restart{Started: now.Started}, nil
			}
		}
	}
}
