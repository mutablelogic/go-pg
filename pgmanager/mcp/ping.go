package mcp

import (
	"context"
	"encoding/json"

	// Packages
	"github.com/mutablelogic/go-llm"
	"github.com/mutablelogic/go-server/pkg/jsonschema"
)

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (mcp *Server) Ping() llm.Tool {
	self := new(tool)
	self.manager = mcp.manager
	self.name = "ping"
	self.title = "Ping"
	self.description = "Ping the PostgreSQL cluster and return health status"
	self.output = jsonschema.MustFor[struct {
		OK bool `json:"ok"`
	}]()
	self.fn = func(ctx context.Context, _ json.RawMessage) (any, error) {
		if err := self.manager.Ping(ctx); err != nil {
			return nil, err
		}
		return struct {
			OK bool `json:"ok"`
		}{OK: true}, nil
	}
	return self
}
