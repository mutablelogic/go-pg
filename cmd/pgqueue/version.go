package main

import (
	"encoding/json"

	// Packages
	"github.com/mutablelogic/go-pg/pkg/version"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func VersionJSON() string {
	metadata := map[string]string{
		"name":       version.ExecName(),
		"version":    version.Version(),
		"compiler":   version.Compiler(),
		"source":     version.GitSource,
		"tag":        version.GitTag,
		"branch":     version.GitBranch,
		"hash":       version.GitHash,
		"build_time": version.GoBuildTime,
	}
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return "{}"
	}
	return string(data)
}
