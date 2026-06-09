// Package schema defines all data types, request/response structures, and SQL
// queries for PostgreSQL management resources.
//
// Each resource type (roles, databases, schemas, objects, etc.) has its own
// file containing Go structs representing PostgreSQL objects, list request
// parameters for filtering and pagination, and methods that produce
// parameterized SQL queries.
package schema
