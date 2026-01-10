package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"sync"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pkg/queue"
	httphandler "github.com/mutablelogic/go-pg/pkg/queue/httphandler"
	version "github.com/mutablelogic/go-pg/pkg/version"
	httpserver "github.com/mutablelogic/go-server/pkg/httpserver"
	"github.com/mutablelogic/go-server/pkg/ref"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ServerCommands struct {
	RunServer RunServer `cmd:"" name:"run" help:"Run server." group:"SERVER"`
}

type RunServer struct {
	URL       string `arg:"" name:"url" help:"Database URL" default:""`
	Namespace string `name:"namespace" help:"Queue namespace" default:""`

	// Postgres options
	PG struct {
		// Database options
		User     string `name:"user" env:"PG_USER" help:"Database user"`
		Password string `name:"password" env:"PG_PASSWORD" help:"Database password"`
		Schema   string `name:"schema" env:"PG_SCHEMA" help:"Database schema"`
	} `embed:"" prefix:"pg."`

	// TLS server options
	TLS struct {
		ServerName string `name:"name" help:"TLS server name"`
		CertFile   string `name:"cert" help:"TLS certificate file"`
		KeyFile    string `name:"key" help:"TLS key file"`
	} `embed:"" prefix:"tls."`
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *RunServer) Run(ctx *Globals) error {
	opts := []pg.Opt{
		pg.WithURL(cmd.URL),
	}
	if cmd.PG.User != "" || cmd.PG.Password != "" {
		opts = append(opts, pg.WithCredentials(cmd.PG.User, cmd.PG.Password))
	}
	if cmd.PG.Schema != "" {
		opts = append(opts, pg.WithSchemaSearchPath(cmd.PG.Schema))
	}

	// Tracing
	if ctx.tracer != nil {
		opts = append(opts, pg.WithTracer(ctx.tracer))
	} else if ctx.Debug {
		opts = append(opts, pg.WithTrace(func(parent context.Context, query string, args any, err error) {
			if err != nil {
				ctx.logger.With("query", query, "args", args).Print(parent, "pg error: ", err)
			} else {
				ctx.logger.With("query", query, "args", args).Debug(parent, "pg trace")
			}
		}))
	}

	// Create a pool connection
	conn, err := pg.NewPool(ctx.ctx, opts...)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Ping the database
	if err := conn.Ping(ctx.ctx); err != nil {
		return err
	}

	// Create the manager
	manager, err := manager.New(ctx.ctx, conn, manager.WithNamespace(cmd.Namespace), manager.WithTracer(ctx.tracer))
	if err != nil {
		return err
	}

	// Set logging middleware
	middleware := httphandler.HTTPMiddlewareFuncs{
		ctx.logger.HandleFunc,
	}

	// If we have an OTEL tracer, add tracing middleware
	if ctx.tracer != nil {
		middleware = append(middleware, otel.HTTPHandlerFunc(ctx.tracer))
	}

	// Register HTTP handlers
	router := http.NewServeMux()
	httphandler.RegisterBackendHandlers(router, ctx.HTTP.Prefix, manager, middleware)
	httphandler.RegisterFrontendHandler(router, "", false, middleware)

	// Create a TLS config
	var tlsconfig *tls.Config
	if cmd.TLS.CertFile != "" || cmd.TLS.KeyFile != "" {
		tlsconfig, err = httpserver.TLSConfig(cmd.TLS.ServerName, true, cmd.TLS.CertFile, cmd.TLS.KeyFile)
		if err != nil {
			return err
		}
	}

	// Create a HTTP server
	server, err := httpserver.New(ctx.HTTP.Addr, router, tlsconfig)
	if err != nil {
		return err
	}

	// We run the manager and the server concurrently
	var wg sync.WaitGroup
	var result error

	// Output the version
	ctx.logger.Printf(ctx.ctx, "%s@%s", version.ExecName(), version.Version())

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := manager.Run(ref.WithLogger(ctx.ctx, ctx.logger)); err != nil {
			if !errors.Is(err, context.Canceled) {
				result = errors.Join(result, fmt.Errorf("queue error: %w", err))
			}
			ctx.cancel()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		// Output listening information
		ctx.logger.With("addr", ctx.HTTP.Addr, "prefix", ctx.HTTP.Prefix).Print(ctx.ctx, "http server starting")

		// Run the server
		if err := server.Run(ctx.ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				result = errors.Join(result, err)
			}
			ctx.cancel()
		}
	}()

	// Wait for both to finish
	wg.Wait()

	// Terminated message
	if result == nil {
		ctx.logger.With("addr", ctx.HTTP.Addr, "prefix", ctx.HTTP.Prefix).Print(ctx.ctx, "http server terminated")
	} else {
		ctx.logger.With("addr", ctx.HTTP.Addr, "prefix", ctx.HTTP.Prefix).Print(ctx.ctx, result)
	}

	// Return any error
	return result
}
