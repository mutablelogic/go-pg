// Package manager provides a comprehensive API for managing PostgreSQL server
// resources including roles, databases, schemas, tables, connections, and more.
//
// It wraps a connection pool and exposes methods for querying and managing
// PostgreSQL system catalogs. The package supports introspection of database
// objects and server configuration.
//
// # Creating a Manager
//
//	mgr, err := manager.New(ctx, pool)
//	if err != nil {
//	    panic(err)
//	}
//
// # Listing Resources
//
// All list operations follow a consistent pattern with filtering and pagination:
//
//	roles, err := mgr.ListRoles(ctx, schema.RoleListRequest{
//	    OffsetLimit: pg.OffsetLimit{Limit: ptr(100)},
//	})
//
// # Managed Resources
//
// The manager provides access to:
//   - Roles (users and groups)
//   - Databases
//   - Schemas
//   - Objects (tables, views, indexes, sequences)
//   - Tablespaces
//   - Extensions
//   - Connections
//   - Settings
//   - Statements (pg_stat_statements)
//   - Replication slots
package manager
