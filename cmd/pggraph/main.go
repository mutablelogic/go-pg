package main

import (
	"fmt"
	"os"

	// Packages
	pggraphcmd "github.com/mutablelogic/go-pg/pggraph/cmd"
	servercmd "github.com/mutablelogic/go-server/pkg/cmd"
	version "github.com/mutablelogic/go-server/pkg/version"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type CLI struct {
	pggraphcmd.ServerCommands // pggraph run
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

const description = "PostgreSQL Graph is an application for running DAG graphs"

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func main() {
	if err := servercmd.Main(CLI{}, description, version.Version()); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}
