package main

import (
	"fmt"
	"os"

	// Packages
	pgqueuecmd "github.com/mutablelogic/go-pg/pgqueue/cmd"
	servercmd "github.com/mutablelogic/go-server/pkg/cmd"
	version "github.com/mutablelogic/go-server/pkg/version"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type CLI struct {
	pgqueuecmd.ClientCommands // pgqueue queues, tickers, tasks
	servercmd.OpenAPICommands // pgqueue openapi
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const description = "pgqueue is a client for task queues, on a server with the pgqueue API"

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func main() {
	if err := servercmd.Main(CLI{}, description, version.Version()); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}
