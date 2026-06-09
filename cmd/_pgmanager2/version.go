package main

import (
	"fmt"

	// Packages
	"github.com/mutablelogic/go-pg/pkg/version"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type VersionCommands struct {
	Version VersionCommand `cmd:"version" help:"Print version information"`
}

type VersionCommand struct{}

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (cmd *VersionCommand) Run(g *Globals) error {
	if version.GitSource != "" {
		fmt.Printf("%s@%s\n\n", version.GitSource, version.Version())
	} else {
		fmt.Printf("pgmanager %s\n\n", version.Version())
	}
	if version.GitHash != "" {
		fmt.Printf("Commit: %s\n", version.GitHash)
	}
	if version.GitBranch != "" {
		fmt.Printf("Branch: %s\n", version.GitBranch)
	}
	if version.GoBuildTime != "" {
		fmt.Printf("Build Time: %s\n", version.GoBuildTime)
	}
	fmt.Printf("Compiler: %s\n", version.Compiler())
	return nil
}
