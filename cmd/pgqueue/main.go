package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"

	// Packages
	kong "github.com/alecthomas/kong"
	client "github.com/mutablelogic/go-client"
	httpclient "github.com/mutablelogic/go-pg/pkg/queue/httpclient"
	"github.com/mutablelogic/go-server"
	"github.com/mutablelogic/go-server/pkg/logger"
	"golang.org/x/crypto/ssh/terminal"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Globals struct {
	// Debug option
	Debug   bool             `name:"debug" help:"Enable debug logging"`
	Version kong.VersionFlag `name:"version" help:"Print version and exit"`

	// HTTP server options
	HTTP struct {
		Prefix string `name:"prefix" help:"HTTP path prefix" default:"/api/v1"`
		Addr   string `name:"addr" env:"PGQUEUE_ADDR" help:"HTTP Listen address" default:":8080"`
	} `embed:"" prefix:"http."`

	// Private fields
	ctx    context.Context
	cancel context.CancelFunc
	logger server.Logger
}

type CLI struct {
	Globals
	NamespaceCommands
	QueueCommands
	TaskCommands
	TickerCommands
	ServerCommands
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func main() {
	cli := new(CLI)
	ctx := kong.Parse(cli,
		kong.Name("pgqueue"),
		kong.Description("pgqueue command line interface"),
		kong.Vars{
			"version": VersionJSON(),
		},
		kong.UsageOnError(),
		kong.ConfigureHelp(kong.HelpOptions{
			Compact: true,
		}),
	)

	// Create the context and cancel function
	cli.Globals.ctx, cli.Globals.cancel = signal.NotifyContext(context.Background(), os.Interrupt)
	defer cli.Globals.cancel()

	// Create a logger
	if isTerminal(os.Stderr) {
		cli.Globals.logger = logger.New(os.Stderr, logger.Term, cli.Debug)
	} else {
		cli.Globals.logger = logger.New(os.Stderr, logger.JSON, cli.Debug)
	}

	// Call the Run() method of the selected parsed command.
	if err := ctx.Run(&cli.Globals); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (g *Globals) Client() (*httpclient.Client, error) {
	scheme := "http"
	host, port, err := net.SplitHostPort(g.HTTP.Addr)
	if err != nil {
		return nil, err
	}

	// Default host to localhost if empty (e.g., ":8080")
	if host == "" {
		host = "localhost"
	}

	// Parse port
	portn, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return nil, err
	}
	if portn == 443 {
		scheme = "https"
	}

	// Client options
	opts := []client.ClientOpt{}
	if g.Debug {
		opts = append(opts, client.OptTrace(os.Stderr, true))
	}

	// Create a client with the calculated endpoint
	return httpclient.New(fmt.Sprintf("%s://%s:%v%s", scheme, host, portn, g.HTTP.Prefix), opts...)
}

func isTerminal(w io.Writer) bool {
	if fd, ok := w.(*os.File); ok {
		return terminal.IsTerminal(int(fd.Fd()))
	}
	return false
}
