package cmd

import (
	"fmt"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pgqueue/manager"
	queue "github.com/mutablelogic/go-pg/pgqueue/manager"
	pgpkg "github.com/mutablelogic/go-pg/pkg/cmd"
	server "github.com/mutablelogic/go-server"
	cmd "github.com/mutablelogic/go-server/pkg/cmd"
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
	Schema              string `name:"schema" prefix:"pg." help:"Database schema to use for pgqueue." env:"PGQUEUE_SCHEMA"`
}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (runner RunServer) Run(ctx server.Cmd) error {
	// Log the version
	ctx.Logger().InfoContext(ctx.Context(), "starting server", "name", ctx.Name(), "version", ctx.Version())

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

		// Run the manager
		errgroup.Go(func() error {
			return pgmanager.Run(errctx, ctx.Logger())
		})

		// Run the server - if any co-routine in the error group returns an error, the server will be shutdown
		errgroup.Go(func() error {
			return runner.RunServer.Run(ctx.WithContext(errctx))
		})

		// Wait for the server and manager to exit, and return any error
		return errgroup.Wait()
	})
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (runner RunServer) WithManager(globals server.Cmd, conn pg.PoolConn, fn func(manager *queue.Manager) error) error {
	opts := []queue.Opt{
		queue.WithMeter(globals.Meter()),
		queue.WithTracer(globals.Tracer()),
	}
	if runner.Schema != "" {
		opts = append(opts, queue.WithSchema(runner.Schema))
	}

	// Create a queue
	manager, err := queue.New(globals.Context(), conn, opts...)
	if err != nil {
		return err
	}

	// Call the function with the manager
	return fn(manager)
}
