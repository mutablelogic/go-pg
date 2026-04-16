package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pkg/manager"
	httphandler "github.com/mutablelogic/go-pg/pkg/manager/httphandler"
	version "github.com/mutablelogic/go-pg/pkg/version"
	httpserver "github.com/mutablelogic/go-server/pkg/httpserver"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ServerCommands struct {
	RunServer RunServer `cmd:"" name:"run" help:"Run server."`
}

type RunServer struct {
	URL string `arg:"" name:"url" help:"Database URL" default:""`
	UI  bool   `name:"ui" help:"Enable frontend UI" default:"false"`

	// Postgres options
	PG struct {
		// Database options
		User     string `name:"user" env:"PG_USER" help:"Database user"`
		Password string `name:"password" env:"PG_PASSWORD" help:"Database password"`
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
	manager, err := manager.New(ctx.ctx, conn)
	if err != nil {
		return err
	}

	// Create a TLS config
	var tlsconfig *tls.Config
	if cmd.TLS.CertFile != "" || cmd.TLS.KeyFile != "" {
		cert, err := os.ReadFile(cmd.TLS.CertFile)
		if err != nil {
			return err
		}
		key, err := os.ReadFile(cmd.TLS.KeyFile)
		if err != nil {
			return err
		}
		tlsconfig, err = httpserver.TLSConfig(cmd.TLS.ServerName, true, cert, key)
		if err != nil {
			return err
		}
	}

	// Create a HTTP server
	server, err := httpserver.New(ctx.HTTP.Addr, tlsconfig)
	if err != nil {
		return err
	}

	// Register HTTP handlers
	router := server.Router()
	httphandler.RegisterBackendHandlers(router, ctx.HTTP.Prefix, manager)
	httphandler.RegisterFrontendHandler(router, "", cmd.UI)

	// Run the server
	fmt.Println(version.ExecName(), version.Version())
	fmt.Println("Listening on", ctx.HTTP.Addr+ctx.HTTP.Prefix)
	return server.Run(ctx.ctx)
}
