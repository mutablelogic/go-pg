package main

import (
	"fmt"
	"os"

	// Packages
	pgmanagercmd "github.com/mutablelogic/go-pg/pgmanager/cmd"
	servercmd "github.com/mutablelogic/go-server/pkg/cmd"
	version "github.com/mutablelogic/go-server/pkg/version"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type CLI struct {
	pgmanagercmd.ClientCommands // pgmanager client commands
	pgmanagercmd.ServerCommands // pgmanager run
	servercmd.OpenAPICommands   // pgmanager openapi
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const description = "Postgresql Manager is an application for managing a database server"

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func main() {
	if err := servercmd.Main(CLI{}, description, version.Version()); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(-1)
	}
}
