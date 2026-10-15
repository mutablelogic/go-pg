package main

import (
	"fmt"
	"os"

	// Packages
	servercmd "github.com/mutablelogic/go-server/pkg/cmd"
	version "github.com/mutablelogic/go-server/pkg/version"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type CLI struct {
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
