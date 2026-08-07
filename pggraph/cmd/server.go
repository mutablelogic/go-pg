package cmd

import (
	"context"
	"fmt"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pggraph/manager"
	"github.com/mutablelogic/go-pg/pggraph/schema"
	pgpkg "github.com/mutablelogic/go-pg/pkg/cmd"
	server "github.com/mutablelogic/go-server"
	cmd "github.com/mutablelogic/go-server/pkg/cmd"
	"github.com/mutablelogic/go-server/pkg/types"
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

	// Report that the server is running
	ctx.Logger().Info("running server", "name", ctx.Name(), "version", ctx.Version())

	// Create the manager, run the server, and return any error
	return runner.WithManager(ctx, conn, func(manager *manager.Manager) error {
		// Create an error context - which will cancel any other goroutine on exit
		errgroup, errctx := errgroup.WithContext(ctx.Context())

		// Add a node to the manager
		_, err := manager.RegisterNode(ctx.Context(), "helloworld", schema.NodeMeta{
			Description: types.Ptr("Prints a greeting message"),
		}, func(ctx context.Context, name string) (string, error) {
			return "Hello, " + name, nil
		})
		if err != nil {
			return err
		}

		// Run the manager
		errgroup.Go(func() error {
			return manager.Run(errctx, ctx.Logger())
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

func (runner *RunServer) WithManager(ctx server.Cmd, conn pg.PoolConn, fn func(*manager.Manager) error) error {
	opts := []manager.Opt{
		manager.WithMeter(ctx.Meter()),
		manager.WithTracer(ctx.Tracer()),
	}
	if manager, err := manager.New(ctx.Context(), conn, ctx.Version(), opts...); err != nil {
		return err
	} else {
		return fn(manager)
	}
}
