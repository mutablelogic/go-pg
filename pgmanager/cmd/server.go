package cmd

import (
	"errors"
	"fmt"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	httphandlers "github.com/mutablelogic/go-pg/pgmanager/httphandlers"
	manager "github.com/mutablelogic/go-pg/pgmanager/manager"
	pgpkg "github.com/mutablelogic/go-pg/pkg/cmd"
	server "github.com/mutablelogic/go-server"
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

	// Create the manager, run the server, and return any error
	return runner.WithManager(ctx, conn, func(pgmanager *manager.Manager) error {
		// Create an error context - which will cancel any other goroutine on exit
		errgroup, errctx := errgroup.WithContext(ctx.Context())

		// Register http handlers for the manager
		runner.Register(func(router *httprouter.Router) error {
			ctx.Logger().DebugContext(ctx.Context(), "registering pgmanager handlers")
			return errors.Join(
				httphandlers.RegisterStatusHandlers(pgmanager, router),
				httphandlers.RegisterRoleHandlers(pgmanager, router),
				httphandlers.RegisterDatabaseHandlers(pgmanager, router),
				httphandlers.RegisterSchemaHandlers(pgmanager, router),
				httphandlers.RegisterTablespaceHandlers(pgmanager, router),
				httphandlers.RegisterConnectionHandlers(pgmanager, router),
				httphandlers.RegisterReplicationSlotHandlers(pgmanager, router),
				httphandlers.RegisterExtensionHandlers(pgmanager, router),
				httphandlers.RegisterSettingHandlers(pgmanager, router),
				router.RegisterCatchAll("/", true),
			)
		})

		// Run the server - if any co-routine in the error group returns an error, the server will be shutdown
		errgroup.Go(func() error {
			return runner.RunServer.Run(ctx.WithContext(errctx))
		})

		// Run the server
		return errgroup.Wait()
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (runner *RunServer) WithManager(ctx server.Cmd, conn pg.PoolConn, fn func(*manager.Manager) error) error {
	opts := []manager.Opt{
		manager.WithMeter(ctx.Meter()),
		manager.WithTracer(ctx.Tracer()),
	}
	if manager, err := manager.New(conn, opts...); err != nil {
		return err
	} else {
		return fn(manager)
	}
}
