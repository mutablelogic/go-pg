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
	pgqueuecmd.ServerCommands // pgqueue run
	servercmd.OpenAPICommands // pgqueue openapi
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const description = "pgqueue is an application for managing task queues"

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func main() {
	if err := servercmd.Main(CLI{}, description, version.Version()); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}
