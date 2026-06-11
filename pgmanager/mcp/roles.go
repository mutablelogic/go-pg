package mcp

import (
	"context"
	"encoding/json"
	"strings"

	// Packages
	"github.com/mutablelogic/go-llm"
	"github.com/mutablelogic/go-pg/pgmanager/schema"
	"github.com/mutablelogic/go-server/pkg/jsonschema"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type ListRolesInput struct {
	Name string `json:"role_name,omitempty" help:"Role name to filter by. Leave empty to list all roles."`
}

type ListRolesOutput struct {
	Roles []*schema.Role `json:"roles"`
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (mcp *Server) ListRoles() llm.Tool {
	self := new(tool)
	self.manager = mcp.manager
	self.name = "list_roles"
	self.title = "List Roles"
	self.description = "List roles in the PostgreSQL cluster, optionally filtering by name"
	self.input = jsonschema.MustFor[ListRolesInput]()
	self.output = jsonschema.MustFor[ListRolesOutput]()
	self.fn = func(ctx context.Context, input json.RawMessage) (any, error) {
		var in ListRolesInput

		if len(input) > 0 && string(input) != "null" {
			if err := json.Unmarshal(input, &in); err != nil {
				return nil, err
			}
		}

		if name := strings.TrimSpace(in.Name); name != "" {
			role, err := self.manager.GetRole(ctx, name)
			if err != nil {
				return nil, err
			}
			return ListRolesOutput{Roles: []*schema.Role{role}}, nil
		}

		list, err := self.manager.ListRoles(ctx, schema.RoleListRequest{})
		if err != nil {
			return nil, err
		}

		result := make([]*schema.Role, 0, len(list.Body))
		for i := range list.Body {
			role := list.Body[i]
			result = append(result, &role)
		}
		return ListRolesOutput{Roles: result}, nil
	}
	return self
}
