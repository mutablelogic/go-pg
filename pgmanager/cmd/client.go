package cmd

import (
	"context"
	"fmt"
	"os"

	// Packages
	otel "github.com/mutablelogic/go-client/pkg/otel"
	httpclient "github.com/mutablelogic/go-pg/pgmanager/httpclient"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	server "github.com/mutablelogic/go-server"
	tui "github.com/mutablelogic/go-server/pkg/tui"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type ClientCommands struct {
	Ping PingCmd `cmd:"" name:"ping" help:"Ping the server." group:"STATUS"`
	RoleClientCommands
	DatabaseClientCommands
	SchemaClientCommands
	ConnectionClientCommands
	ExtensionClientCommands
	SettingClientCommands
}

type PingCmd struct{}

type RoleClientCommands struct {
	RoleList   RoleListCmd   `cmd:"" name:"roles" help:"List roles." group:"ROLE"`
	RoleGet    RoleGetCmd    `cmd:"" name:"role" help:"Get role details." group:"ROLE"`
	RoleCreate RoleCreateCmd `cmd:"" name:"role-create" help:"Create a new role." group:"ROLE"`
	RoleDelete RoleDeleteCmd `cmd:"" name:"role-delete" help:"Delete a role." group:"ROLE"`
	RoleUpdate RoleUpdateCmd `cmd:"" name:"role-update" help:"Update a role." group:"ROLE"`
}

type DatabaseClientCommands struct {
	DatabaseList   DatabaseListCmd   `cmd:"" name:"databases" help:"List databases." group:"DATABASE"`
	DatabaseGet    DatabaseGetCmd    `cmd:"" name:"database" help:"Get database details." group:"DATABASE"`
	DatabaseCreate DatabaseCreateCmd `cmd:"" name:"database-create" help:"Create a new database." group:"DATABASE"`
	DatabaseDelete DatabaseDeleteCmd `cmd:"" name:"database-delete" help:"Delete a database." group:"DATABASE"`
	DatabaseUpdate DatabaseUpdateCmd `cmd:"" name:"database-update" help:"Update a database." group:"DATABASE"`
}

type SchemaClientCommands struct {
	SchemaList   SchemaListCmd   `cmd:"" name:"schemas" help:"List schemas." group:"SCHEMA"`
	SchemaGet    SchemaGetCmd    `cmd:"" name:"schema" help:"Get schema details." group:"SCHEMA"`
	SchemaCreate SchemaCreateCmd `cmd:"" name:"schema-create" help:"Create a new schema in a database." group:"SCHEMA"`
	SchemaDelete SchemaDeleteCmd `cmd:"" name:"schema-delete" help:"Delete a schema from a database." group:"SCHEMA"`
	SchemaUpdate SchemaUpdateCmd `cmd:"" name:"schema-update" help:"Update a schema in a database." group:"SCHEMA"`
}

type ConnectionClientCommands struct {
	ConnectionList   ConnectionListCmd   `cmd:"" name:"connections" help:"List connections." group:"CONNECTION"`
	ConnectionGet    ConnectionGetCmd    `cmd:"" name:"connection" help:"Get connection details." group:"CONNECTION"`
	ConnectionDelete ConnectionDeleteCmd `cmd:"" name:"connection-delete" help:"Delete a connection." group:"CONNECTION"`
}

type ExtensionClientCommands struct {
	ExtensionList   ExtensionListCmd   `cmd:"" name:"extensions" help:"List extensions." group:"EXTENSION"`
	ExtensionGet    ExtensionGetCmd    `cmd:"" name:"extension" help:"Get extension details." group:"EXTENSION"`
	ExtensionCreate ExtensionCreateCmd `cmd:"" name:"extension-install" help:"Install an extension into a database schema." group:"EXTENSION"`
	ExtensionDelete ExtensionDeleteCmd `cmd:"" name:"extension-remove" help:"Remove an extension from one or more database schemas." group:"EXTENSION"`
}

type SettingClientCommands struct {
	SettingList         SettingListCmd         `cmd:"" name:"settings" help:"List server settings." group:"SETTING"`
	SettingCategoryList SettingCategoryListCmd `cmd:"" name:"categories" help:"List distinct setting categories." group:"SETTING"`
	SettingGet          SettingGetCmd          `cmd:"" name:"setting" help:"Get setting details." group:"SETTING"`
	SettingUpdate       SettingUpdateCmd       `cmd:"" name:"setting-update" help:"Update a setting." group:"SETTING"`
}

type RoleListCmd struct {
	schema.RoleListRequest
}

type RoleCreateCmd struct {
	schema.RoleMeta
}

type RoleGetCmd struct {
	Name string `arg:"" name:"name" help:"Name of the role."`
}

type RoleDeleteCmd struct {
	Name string `arg:"" name:"name" help:"Name of the role."`
}

type RoleUpdateCmd struct {
	NewName string `flag:"" name:"role" help:"New name for the role."`
	schema.RoleMeta
}

type DatabaseListCmd struct {
	schema.DatabaseListRequest
}

type DatabaseGetCmd struct {
	Name string `arg:"" name:"name" help:"Name of the database."`
}

type DatabaseCreateCmd struct {
	schema.DatabaseMeta
}

type DatabaseDeleteCmd struct {
	Name string `arg:"" name:"name" help:"Name of the database."`
}

type DatabaseUpdateCmd struct {
	NewName string `flag:"" name:"name" help:"New name of the database."`
	schema.DatabaseMeta
}

type SchemaListCmd struct {
	schema.SchemaListRequest
}

type SchemaGetCmd struct {
	Database  string `arg:"" name:"database" help:"Name of the database."`
	Namespace string `arg:"" name:"schema" help:"Name of the schema."`
}

type SchemaCreateCmd struct {
	Database string `arg:"" name:"database" help:"Name of the database."`
	schema.SchemaMeta
}

type SchemaDeleteCmd struct {
	Database  string `arg:"" name:"database" help:"Name of the database."`
	Namespace string `arg:"" name:"schema" help:"Name of the schema."`
	Force     bool   `flag:"" name:"force" help:"Force deletion of the schema."`
}

type SchemaUpdateCmd struct {
	Database     string `arg:"" name:"database" help:"Name of the database."`
	NewNamespace string `flag:"" name:"schema" help:"New name of the schema."`
	schema.SchemaMeta
}

type ConnectionListCmd struct {
	schema.ConnectionListRequest
}

type ConnectionGetCmd struct {
	Pid uint64 `arg:"" name:"pid" help:"PID of the connection."`
}

type ConnectionDeleteCmd struct {
	Pid uint64 `arg:"" name:"pid" help:"PID of the connection."`
}

type ExtensionListCmd struct {
	schema.ExtensionListRequest
}

type ExtensionGetCmd struct {
	Name string `arg:"" name:"name" help:"Name of the extension."`
}

type ExtensionCreateCmd struct {
	schema.ExtensionMeta
	Cascade bool `flag:"" name:"cascade" help:"Cascade option."`
}

type ExtensionDeleteCmd struct {
	Name    string `arg:"" name:"name" help:"Name of the extension."`
	Cascade bool   `flag:"" name:"cascade" help:"Cascade option."`
}

type SettingListCmd struct {
	schema.SettingListRequest
}

type SettingCategoryListCmd struct {
	schema.SettingCategoryListRequest
}

type SettingGetCmd struct {
	Name string `arg:"" name:"name" help:"Name of the setting."`
}

type SettingUpdateCmd struct {
	Name string `arg:"" name:"name" help:"Name of the setting."`
	schema.SettingMeta
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func withClient(ctx server.Cmd, span string, fn func(context.Context, *httpclient.Client) error) error {
	endpoint, opts, err := ctx.ClientEndpoint()
	if err != nil {
		return err
	} else if client, err := httpclient.New(endpoint, opts...); err != nil {
		return err
	} else {
		var err error
		ctx, endfn := otel.StartSpan(ctx.Tracer(), ctx.Context(), span)
		defer func() { endfn(err) }()
		err = fn(ctx, client)
		return err
	}
}

///////////////////////////////////////////////////////////////////////////////
// STATUS COMMANDS

func (cmd *PingCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "ping", func(ctx context.Context, client *httpclient.Client) error {
		return client.Ping(ctx)
	})
}

///////////////////////////////////////////////////////////////////////////////
// ROLE COMMANDS

func (cmd *RoleListCmd) Run(ctx server.Cmd) error {
	// Set the width of the terminal
	width := ctx.IsTerm()

	// Perform the request
	return withClient(ctx, "roles", func(ctx context.Context, client *httpclient.Client) error {
		roles, err := client.ListRoles(ctx, cmd.RoleListRequest)
		if err != nil {
			return err
		}

		// Roles list table
		table := tui.TableFor[schema.Role](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, roles.Body...); err != nil {
			return err
		}

		// Roles list summary
		summary := tui.TableSummary("roles", uint(roles.Count), roles.Offset, roles.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

func (cmd *RoleCreateCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "role-create", func(ctx context.Context, client *httpclient.Client) error {
		role, err := client.CreateRole(ctx, cmd.RoleMeta)
		if err != nil {
			return err
		}

		fmt.Println(role)
		return nil
	})
}

func (cmd *RoleGetCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "role", func(ctx context.Context, client *httpclient.Client) error {
		role, err := client.GetRole(ctx, cmd.Name)
		if err != nil {
			return err
		}

		fmt.Println(role)
		return nil
	})
}

func (cmd *RoleDeleteCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "role-delete", func(ctx context.Context, client *httpclient.Client) error {
		if _, err := client.DeleteRole(ctx, cmd.Name); err != nil {
			return err
		}
		return nil
	})
}

func (cmd *RoleUpdateCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "role-update", func(ctx context.Context, client *httpclient.Client) error {
		// We swap the name in the meta with the new name
		cmd.NewName, cmd.RoleMeta.Name = cmd.RoleMeta.Name, cmd.NewName

		// Perform the update
		role, err := client.UpdateRole(ctx, cmd.NewName, cmd.RoleMeta)
		if err != nil {
			return err
		}

		fmt.Println(role)
		return nil
	})
}

///////////////////////////////////////////////////////////////////////////////
// DATABASE COMMANDS

func (cmd *DatabaseListCmd) Run(ctx server.Cmd) error {
	// Set the width of the terminal
	width := ctx.IsTerm()

	// Perform the request
	return withClient(ctx, "databases", func(ctx context.Context, client *httpclient.Client) error {
		databases, err := client.ListDatabases(ctx, cmd.DatabaseListRequest)
		if err != nil {
			return err
		}

		// Databases list table
		table := tui.TableFor[schema.Database](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, databases.Body...); err != nil {
			return err
		}

		// Databases list summary
		summary := tui.TableSummary("databases", uint(databases.Count), databases.Offset, databases.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

func (cmd *DatabaseGetCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "database", func(ctx context.Context, client *httpclient.Client) error {
		database, err := client.GetDatabase(ctx, cmd.Name)
		if err != nil {
			return err
		}

		fmt.Println(database)
		return nil
	})
}

func (cmd *DatabaseCreateCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "database-create", func(ctx context.Context, client *httpclient.Client) error {
		database, err := client.CreateDatabase(ctx, cmd.DatabaseMeta)
		if err != nil {
			return err
		}

		fmt.Println(database)
		return nil
	})
}

func (cmd *DatabaseDeleteCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "database-delete", func(ctx context.Context, client *httpclient.Client) error {
		return client.DeleteDatabase(ctx, cmd.Name, false)
	})
}

func (cmd *DatabaseUpdateCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "database-update", func(ctx context.Context, client *httpclient.Client) error {
		// We swap the name in the meta with the new name
		cmd.Name, cmd.NewName = cmd.NewName, cmd.Name

		// Perform the update
		database, err := client.UpdateDatabase(ctx, cmd.NewName, cmd.DatabaseMeta)
		if err != nil {
			return err
		}

		fmt.Println(database)
		return nil
	})
}

///////////////////////////////////////////////////////////////////////////////
// SCHEMA COMMANDS

func (cmd *SchemaListCmd) Run(ctx server.Cmd) error {
	// Set the width of the terminal
	width := ctx.IsTerm()

	// Perform the request
	return withClient(ctx, "schemas", func(ctx context.Context, client *httpclient.Client) error {
		schemas, err := client.ListSchemas(ctx, cmd.SchemaListRequest)
		if err != nil {
			return err
		}

		// Schemas list table
		table := tui.TableFor[schema.Schema](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, schemas.Body...); err != nil {
			return err
		}

		// Schemas list summary
		summary := tui.TableSummary("schemas", uint(schemas.Count), schemas.Offset, schemas.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

func (cmd *SchemaGetCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "schema", func(ctx context.Context, client *httpclient.Client) error {
		schema, err := client.GetSchema(ctx, cmd.Database, cmd.Namespace)
		if err != nil {
			return err
		}

		fmt.Println(schema)
		return nil
	})
}

func (cmd *SchemaCreateCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "schema-create", func(ctx context.Context, client *httpclient.Client) error {
		schema, err := client.CreateSchema(ctx, cmd.Database, cmd.SchemaMeta)
		if err != nil {
			return err
		}

		fmt.Println(schema)
		return nil
	})
}

func (cmd *SchemaDeleteCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "schema-delete", func(ctx context.Context, client *httpclient.Client) error {
		if _, err := client.DeleteSchema(ctx, cmd.Database, cmd.Namespace, cmd.Force); err != nil {
			return err
		}
		return nil
	})
}

func (cmd *SchemaUpdateCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "schema-update", func(ctx context.Context, client *httpclient.Client) error {
		// We swap the name in the meta with the new name
		cmd.Name, cmd.NewNamespace = cmd.NewNamespace, cmd.Name

		// Perform the update
		schema, err := client.UpdateSchema(ctx, cmd.Database, cmd.NewNamespace, cmd.SchemaMeta)
		if err != nil {
			return err
		}

		fmt.Println(schema)
		return nil
	})
}

///////////////////////////////////////////////////////////////////////////////
// CONNECTION COMMANDS

func (cmd *ConnectionListCmd) Run(ctx server.Cmd) error {
	// Set the width of the terminal
	width := ctx.IsTerm()

	// Perform the request
	return withClient(ctx, "connections", func(ctx context.Context, client *httpclient.Client) error {
		connections, err := client.ListConnections(ctx, cmd.ConnectionListRequest)
		if err != nil {
			return err
		}

		// Connections list table
		table := tui.TableFor[schema.Connection](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, connections.Body...); err != nil {
			return err
		}

		// Connections list summary
		summary := tui.TableSummary("connections", uint(connections.Count), connections.Offset, connections.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

func (cmd *ConnectionGetCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "connection", func(ctx context.Context, client *httpclient.Client) error {
		connection, err := client.GetConnection(ctx, cmd.Pid)
		if err != nil {
			return err
		}

		fmt.Println(connection)
		return nil
	})
}

func (cmd *ConnectionDeleteCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "connection-delete", func(ctx context.Context, client *httpclient.Client) error {
		return client.DeleteConnection(ctx, cmd.Pid)
	})
}

///////////////////////////////////////////////////////////////////////////////
// EXTENSION COMMANDS

func (cmd *ExtensionListCmd) Run(ctx server.Cmd) error {
	// Set the width of the terminal
	width := ctx.IsTerm()

	// Perform the request
	return withClient(ctx, "extensions", func(ctx context.Context, client *httpclient.Client) error {
		extensions, err := client.ListExtensions(ctx, cmd.ExtensionListRequest)
		if err != nil {
			return err
		}

		// Extensions list table
		table := tui.TableFor[schema.Extension](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, extensions.Body...); err != nil {
			return err
		}

		// Extensions list summary
		summary := tui.TableSummary("extensions", uint(extensions.Count), extensions.Offset, extensions.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

func (cmd *ExtensionGetCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "extension", func(ctx context.Context, client *httpclient.Client) error {
		extension, err := client.GetExtension(ctx, cmd.Name)
		if err != nil {
			return err
		}

		fmt.Println(extension)
		return nil
	})
}

func (cmd *ExtensionCreateCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "extension-install", func(ctx context.Context, client *httpclient.Client) error {
		extension, err := client.CreateExtension(ctx, cmd.ExtensionMeta, cmd.Cascade)
		if err != nil {
			return err
		}

		fmt.Println(extension)
		return nil
	})
}

///////////////////////////////////////////////////////////////////////////////
// SETTING COMMANDS

func (cmd *SettingListCmd) Run(ctx server.Cmd) error {
	// Set the width of the terminal
	width := ctx.IsTerm()

	// Perform the request
	return withClient(ctx, "settings", func(ctx context.Context, client *httpclient.Client) error {
		settings, err := client.ListSettings(ctx, cmd.SettingListRequest)
		if err != nil {
			return err
		}

		// Settings list table
		table := tui.TableFor[schema.Setting](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, settings.Body...); err != nil {
			return err
		}

		// Settings list summary
		summary := tui.TableSummary("settings", uint(settings.Count), settings.Offset, settings.Limit)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

func (cmd *SettingCategoryListCmd) Run(ctx server.Cmd) error {
	// Set the width of the terminal
	width := ctx.IsTerm()

	// Perform the request
	return withClient(ctx, "setting-categories", func(ctx context.Context, client *httpclient.Client) error {
		categories, err := client.ListSettingCategories(ctx, cmd.SettingCategoryListRequest)
		if err != nil {
			return err
		}

		// Comvert string to schema.CategoryName for table rendering
		categoryNames := make([]schema.CategoryName, len(categories.Body))
		for i, category := range categories.Body {
			categoryNames[i] = schema.CategoryName(category)
		}

		// Categories list table
		table := tui.TableFor[schema.CategoryName](tui.SetWidth(width))
		if _, err := table.Write(os.Stdout, categoryNames...); err != nil {
			return err
		}

		// Categories list summary
		summary := tui.TableSummary("categories", uint(categories.Count), 0, nil)
		if _, err := summary.Write(os.Stdout); err != nil {
			return err
		}

		return nil
	})
}

func (cmd *SettingGetCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "setting", func(ctx context.Context, client *httpclient.Client) error {
		setting, err := client.GetSetting(ctx, cmd.Name)
		if err != nil {
			return err
		}

		fmt.Println(setting)
		return nil
	})
}

func (cmd *SettingUpdateCmd) Run(ctx server.Cmd) error {
	return withClient(ctx, "setting-update", func(ctx context.Context, client *httpclient.Client) error {
		setting, err := client.UpdateSetting(ctx, cmd.Name, cmd.SettingMeta)
		if err != nil {
			return err
		}

		fmt.Println(setting)
		return nil
	})
}
