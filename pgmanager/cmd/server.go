package cmd

import (
	"errors"
	"fmt"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	frontend "github.com/mutablelogic/go-pg/frontend/cmd"
	httphandlers "github.com/mutablelogic/go-pg/pgmanager/httphandlers"
	manager "github.com/mutablelogic/go-pg/pgmanager/manager"
	pgpkg "github.com/mutablelogic/go-pg/pkg/cmd"
	server "github.com/mutablelogic/go-server"
	broadcaster "github.com/mutablelogic/go-server/broadcaster"
	broadcastercmd "github.com/mutablelogic/go-server/broadcaster/cmd"
	cmd "github.com/mutablelogic/go-server/pkg/cmd"
	httprouter "github.com/mutablelogic/go-server/pkg/httprouter"
	errgroup "golang.org/x/sync/errgroup"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ServerCommands struct {
	Run RunServer `cmd:"" name:"run" help:"Run the server." group:"SERVER"`
}

type RunServer struct {
	cmd.RunServer
	pgpkg.PostgresFlags `embed:"" prefix:"pg."`

	// Restart lets pgmanager restart the server, beside which it runs,
	// under a supervisor, which starts it again
	Restart     bool                 `name:"pg.restart" env:"PG_RESTART" help:"Let pgmanager restart PostgreSQL, which it must run beside, as the same user, or root, with a supervisor which starts it again"`
	Broadcaster broadcastercmd.Flags `embed:"" prefix:"broadcaster."`
	Frontend    frontend.Flags       `embed:""`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (runner *RunServer) Run(ctx server.Cmd) error {
	// Connect to the database, if configured
	conn, err := runner.PostgresFlags.Connect(ctx)
	if err != nil {
		return err
	} else if conn == nil {
		return fmt.Errorf("database connection is required")
	}

	// Create the manager, and the broadcaster of changes, which the web UI
	// follows the server with, run the server, and return any error
	return runner.Broadcaster.WithBroadcaster(ctx, &runner.RunServer, func(changes *broadcaster.Broadcaster) error {
		return runner.WithManager(ctx, conn, func(pgmanager *manager.Manager) error {
			return runner.run(ctx, pgmanager, changes)
		})
	})
}

// run runs the manager, the broadcaster, and the server, until any of them
// stops
func (runner *RunServer) run(ctx server.Cmd, pgmanager *manager.Manager, changes *broadcaster.Broadcaster) error {
	// Create an error context - which will cancel any other goroutine on exit
	errgroup, errctx := errgroup.WithContext(ctx.Context())

	// Run the manager, and the broadcaster
	errgroup.Go(func() error {
		return pgmanager.Run(errctx)
	})
	errgroup.Go(func() error {
		return changes.Run(errctx)
	})

	// Run the server - if any co-routine in the error group returns an error, the server will be shutdown
	errgroup.Go(func() error {
		return runner.RunServer.Run(ctx.WithContext(errctx))
	})

	// Wait for the server, the manager and the broadcaster to exit, and
	// return any error
	return errgroup.Wait()
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// WithManager creates a manager, with the server's tracer and meter,
// registers its HTTP handlers with the server, and then calls the function
// with it, returning any error
func (runner *RunServer) WithManager(ctx server.Cmd, conn pg.PoolConn, fn func(*manager.Manager) error) error {
	opts := []manager.Opt{
		manager.WithMeter(ctx.Meter()),
		manager.WithTracer(ctx.Tracer()),
		manager.WithRestart(runner.Restart),
	}
	pgmanager, err := manager.New(conn, opts...)
	if err != nil {
		return err
	}

	// Register http handlers for the manager, the configuration, and the web
	// UI, when it's served, or else a handler of / which isn't found
	runner.Register(func(router *httprouter.Router) error {
		ctx.Logger().DebugContext(ctx.Context(), "registering pgmanager handlers")
		ui, err := runner.Frontend.RegisterUI(ctx, ctx.Logger(), router, runner.RunServer.OpenAPI)
		if err != nil {
			return err
		}
		// Without the UI, nothing else is found, as JSON. With it, the UI's
		// files are, and the server catches what isn't found beneath the
		// API's prefix.
		if !ui {
			if err := router.RegisterCatchAll("/", true); err != nil {
				return err
			}
		}
		return errors.Join(
			httphandlers.RegisterStatusHandlers(pgmanager, router),
			httphandlers.RegisterRoleHandlers(pgmanager, router),
			httphandlers.RegisterDatabaseHandlers(pgmanager, router),
			httphandlers.RegisterSchemaHandlers(pgmanager, router),
			httphandlers.RegisterObjectHandlers(pgmanager, router),
			httphandlers.RegisterStatementHandlers(pgmanager, router),
			httphandlers.RegisterTablespaceHandlers(pgmanager, router),
			httphandlers.RegisterConnectionHandlers(pgmanager, router),
			httphandlers.RegisterReplicationSlotHandlers(pgmanager, router),
			httphandlers.RegisterExtensionHandlers(pgmanager, router),
			httphandlers.RegisterSettingHandlers(pgmanager, router),
		)
	})

	// Call the function with the manager
	return fn(pgmanager)
}
