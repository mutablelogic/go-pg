package mcp

import (
	"context"
	"encoding/json"

	// Packages
	llm "github.com/mutablelogic/go-llm"
	mcp "github.com/mutablelogic/go-llm/mcp/server"
	manager "github.com/mutablelogic/go-pg/pgmanager/manager"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	jsonschema "github.com/mutablelogic/go-server/pkg/jsonschema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Server struct {
	*mcp.Server
	manager *manager.Manager
}

type tool struct {
	name        string
	title       string
	description string
	input       *jsonschema.Schema
	output      *jsonschema.Schema
	fn          func(ctx context.Context, input json.RawMessage) (any, error)
	manager     *manager.Manager
}

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(manager *manager.Manager, name, version string, opts ...mcp.ServerOpt) (*Server, error) {
	server, err := mcp.New(name, version, opts...)
	if err != nil {
		return nil, err
	}

	// Create a server
	self := new(Server)
	self.Server = server
	self.manager = manager

	// Add tools to the server
	if err := self.AddTools(self.Ping(), self.ListRoles()); err != nil {
		return nil, err
	}

	// Return success
	return self, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS - IMPLEMENTATION

func (tool *tool) Name() string {
	return tool.name
}

func (tool *tool) Description() string {
	return tool.description
}

func (tool *tool) InputSchema() *jsonschema.Schema {
	if tool.input == nil {
		return jsonschema.MustFor[struct{}]()
	}
	return tool.input
}

func (tool *tool) OutputSchema() *jsonschema.Schema {
	return tool.output
}

func (tool *tool) Meta() llm.ToolMeta {
	return llm.ToolMeta{Title: tool.title}
}

func (tool *tool) Run(ctx context.Context, input json.RawMessage) (any, error) {
	if tool.fn == nil {
		return nil, httpresponse.ErrNotImplemented.Withf("tool %q is not implemented", tool.name)
	}
	return tool.fn(ctx, input)
}
