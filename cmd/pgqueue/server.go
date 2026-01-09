package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"sync"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pkg/queue"
	httphandler "github.com/mutablelogic/go-pg/pkg/queue/httphandler"
	version "github.com/mutablelogic/go-pg/pkg/version"
	httpserver "github.com/mutablelogic/go-server/pkg/httpserver"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ServerCommands struct {
	RunServer RunServer `cmd:"" name:"run" help:"Run server." group:"SERVER"`
}

type RunServer struct {
	URL       string `arg:"" name:"url" help:"Database URL" default:""`
	Namespace string `name:"namespace" help:"Queue namespace" default:"default"`

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
	if ctx.Debug {
		opts = append(opts, pg.WithTrace(func(ctx context.Context, query string, args any, err error) {
			fmt.Println("PG TRACE:", query, args, err)
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
	manager, err := manager.New(ctx.ctx, conn, cmd.Namespace)
	if err != nil {
		return err
	}

	// Register HTTP handlers
	router := http.NewServeMux()
	httphandler.RegisterBackendHandlers(router, ctx.HTTP.Prefix, manager)
	httphandler.RegisterFrontendHandler(router, "", false)

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
	fmt.Println(version.ExecName(), version.Version())

	wg.Add(1)
	go func() {
		defer wg.Done()

		if err := manager.Run(ctx.ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				result = errors.Join(result, fmt.Errorf("queue error: %w", err))
			}
			ctx.cancel()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		fmt.Println("...listening on", ctx.HTTP.Addr+ctx.HTTP.Prefix)
		if err := server.Run(ctx.ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				result = errors.Join(result, fmt.Errorf("server error: %w", err))
			}
			ctx.cancel()
		}
	}()

	// Wait for both to finish
	wg.Wait()

	// Terminated message
	if result == nil {
		fmt.Println(version.ExecName(), "terminated")
	}

	// Return any error
	return result
}
