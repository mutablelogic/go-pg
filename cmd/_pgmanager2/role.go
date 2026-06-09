package main

import (
	"fmt"

	// Packages
	httpclient "github.com/mutablelogic/go-pg/pkg/manager/httpclient"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type RoleCommands struct {
	ListRole   ListRoleCommand   `cmd:"" name:"roles" help:"List roles."`
	GetRole    GetRoleCommand    `cmd:"" name:"role" help:"Get role."`
	CreateRole CreateRoleCommand `cmd:"" name:"create-role" help:"Create role."`
	DeleteRole DeleteRoleCommand `cmd:"" name:"delete-role" help:"Delete role."`
	UpdateRole UpdateRoleCommand `cmd:"" name:"update-role" help:"Update role."`
}

type ListRoleCommand struct {
	Offset uint64  `name:"offset" help:"Offset for pagination"`
	Limit  *uint64 `name:"limit" help:"Limit for pagination"`
}

type GetRoleCommand struct {
	Name string `arg:"" name:"name" help:"Role name"`
}

type DeleteRoleCommand struct {
	GetRoleCommand
}

type CreateRoleCommand struct {
	GetRoleCommand
	Superuser       bool     `name:"superuser" help:"Superuser permission"`
	NoSuperuser     bool     `name:"no-superuser" help:"No superuser permission"`
	Login           bool     `name:"login" help:"Login permission"`
	NoLogin         bool     `name:"no-login" help:"No login permission"`
	CreateDB        bool     `name:"createdb" help:"Create database permission"`
	NoCreateDB      bool     `name:"no-createdb" help:"No create database permission"`
	CreateRole      bool     `name:"createrole" help:"Create role permission"`
	NoCreateRole    bool     `name:"no-createrole" help:"No create role permission"`
	Replication     bool     `name:"replication" help:"Replication permission"`
	NoReplication   bool     `name:"no-replication" help:"No replication permission"`
	Inherit         bool     `name:"inherit" help:"Inherit permissions from groups"`
	NoInherit       bool     `name:"no-inherit" help:"Do not inherit permissions from groups"`
	BypassRLS       bool     `name:"bypassrls" help:"Bypass row-level security"`
	NoBypassRLS     bool     `name:"no-bypassrls" help:"Do not bypass row-level security"`
	ConnectionLimit *uint64  `name:"connection-limit" help:"Connection limit (-1 for unlimited)"`
	Password        string   `name:"password" help:"Role password"`
	Groups          []string `name:"memberof" help:"Group memberships (role names)"`
}

type UpdateRoleCommand struct {
	GetRoleCommand
	NewName         string   `name:"rename" help:"Rename role to this name"`
	Superuser       *bool    `name:"superuser" help:"Superuser permission"`
	Login           *bool    `name:"login" help:"Login permission"`
	CreateDB        *bool    `name:"createdb" help:"Create database permission"`
	CreateRole      *bool    `name:"createrole" help:"Create role permission"`
	Replication     *bool    `name:"replication" help:"Replication permission"`
	Inherit         *bool    `name:"inherit" help:"Inherit permissions from groups"`
	BypassRLS       *bool    `name:"bypassrls" help:"Bypass row-level security"`
	ConnectionLimit *uint64  `name:"connection-limit" help:"Connection limit (-1 for unlimited)"`
	Password        string   `name:"password" help:"Role password"`
	Groups          []string `name:"memberof" help:"Group memberships (role names)"`
}

///////////////////////////////////////////////////////////////////////////////
// COMMANDS

func (cmd *ListRoleCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// List roles
	roles, err := client.ListRoles(ctx.ctx, httpclient.WithOffsetLimit(cmd.Offset, cmd.Limit))
	if err != nil {
		return err
	}

	// Print
	fmt.Println(roles)
	return nil
}

func (cmd *GetRoleCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Get one role
	role, err := client.GetRole(ctx.ctx, cmd.Name)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(role)
	return nil
}

func (cmd *CreateRoleCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Build role meta
	meta := schema.RoleMeta{
		Name:   cmd.Name,
		Groups: cmd.Groups,
	}

	// Handle boolean flags with explicit true/false
	if cmd.Superuser {
		t := true
		meta.Superuser = &t
	} else if cmd.NoSuperuser {
		f := false
		meta.Superuser = &f
	}
	if cmd.Login {
		t := true
		meta.Login = &t
	} else if cmd.NoLogin {
		f := false
		meta.Login = &f
	}
	if cmd.CreateDB {
		t := true
		meta.CreateDatabases = &t
	} else if cmd.NoCreateDB {
		f := false
		meta.CreateDatabases = &f
	}
	if cmd.CreateRole {
		t := true
		meta.CreateRoles = &t
	} else if cmd.NoCreateRole {
		f := false
		meta.CreateRoles = &f
	}
	if cmd.Replication {
		t := true
		meta.Replication = &t
	} else if cmd.NoReplication {
		f := false
		meta.Replication = &f
	}
	if cmd.Inherit {
		t := true
		meta.Inherit = &t
	} else if cmd.NoInherit {
		f := false
		meta.Inherit = &f
	}
	if cmd.BypassRLS {
		t := true
		meta.BypassRowLevelSecurity = &t
	} else if cmd.NoBypassRLS {
		f := false
		meta.BypassRowLevelSecurity = &f
	}
	if cmd.ConnectionLimit != nil {
		meta.ConnectionLimit = cmd.ConnectionLimit
	}
	if cmd.Password != "" {
		meta.Password = &cmd.Password
	}

	// Create role
	role, err := client.CreateRole(ctx.ctx, meta)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(role)
	return nil
}

func (cmd *DeleteRoleCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Delete role
	if err := client.DeleteRole(ctx.ctx, cmd.Name); err != nil {
		return err
	}

	// Return success
	return nil
}

func (cmd *UpdateRoleCommand) Run(ctx *Globals) error {
	client, err := ctx.Client()
	if err != nil {
		return err
	}

	// Build role meta
	meta := schema.RoleMeta{
		Groups: cmd.Groups,
	}
	if cmd.NewName != "" {
		meta.Name = cmd.NewName
	}
	if cmd.Superuser != nil {
		meta.Superuser = cmd.Superuser
	}
	if cmd.Login != nil {
		meta.Login = cmd.Login
	}
	if cmd.CreateDB != nil {
		meta.CreateDatabases = cmd.CreateDB
	}
	if cmd.CreateRole != nil {
		meta.CreateRoles = cmd.CreateRole
	}
	if cmd.Replication != nil {
		meta.Replication = cmd.Replication
	}
	if cmd.Inherit != nil {
		meta.Inherit = cmd.Inherit
	}
	if cmd.BypassRLS != nil {
		meta.BypassRowLevelSecurity = cmd.BypassRLS
	}
	if cmd.ConnectionLimit != nil {
		meta.ConnectionLimit = cmd.ConnectionLimit
	}
	if cmd.Password != "" {
		meta.Password = &cmd.Password
	}

	// Update role
	role, err := client.UpdateRole(ctx.ctx, cmd.Name, meta)
	if err != nil {
		return err
	}

	// Print
	fmt.Println(role)
	return nil
}
