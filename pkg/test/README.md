# Testing Support

The `test` package provides utilities for integration testing with PostgreSQL using [testcontainers-go](https://github.com/testcontainers/testcontainers-go). It automatically starts a PostgreSQL container, creates a connection pool, and cleans up after tests complete.

## Usage

### TestMain Pattern

For test suites that share a single container across all tests in a package:

```go
package mypackage_test

import (
  "testing"

  pg "github.com/mutablelogic/go-pg"
  pgtest "github.com/mutablelogic/go-pg/pkg/test"
)

var conn pgtest.Conn

func TestMain(m *testing.M) {
  pgtest.Main(m, &conn)
}

func TestSomething(t *testing.T) {
  c := conn.Begin(t)
  defer c.Close()

  // Use c.PoolConn for database operations
  err := c.Exec(ctx, "CREATE TABLE test (id SERIAL PRIMARY KEY)")
  // ...
}
```

### Per-Test Container

For isolated tests that need their own container:

```go
func TestWithManager(t *testing.T) {
  mgr := pgtest.NewManager(t)
  defer mgr.Close()

  // Use mgr.Manager for PostgreSQL management operations
  roles, err := mgr.ListRoles(ctx, schema.RoleListRequest{})
  // ...
}
```

## Container Options

When creating containers directly, you can customize the configuration:

```go
container, err := pgtest.NewContainer(ctx, "mytest", "postgres:16",
  pgtest.WithEnv("POSTGRES_PASSWORD", "secret"),
  pgtest.WithEnv("POSTGRES_DB", "testdb"),
  pgtest.WithPort("5432/tcp"),
  pgtest.WithWaitLog("database system is ready to accept connections"),
)
```

### Available Options

| Option | Description |
|--------|-------------|
| `WithEnv(key, value)` | Set environment variable |
| `WithPort(port)` | Expose a port (e.g., "5432/tcp") |
| `WithWaitLog(message)` | Wait for log message before considering container ready |
| `WithWaitPort(port)` | Wait for port to be available |

## PostgreSQL Container

The `NewPgxContainer` function provides a preconfigured PostgreSQL container:

```go
container, pool, err := pgtest.NewPgxContainer(ctx, "mytest", verbose, traceFn)
if err != nil {
  t.Fatal(err)
}
defer pool.Close()
defer container.Close(ctx)
```

With optional schema search path:

```go
container, pool, err := pgtest.NewPgxContainer(ctx, "mytest", verbose, traceFn, "myschema", "public")
if err != nil {
  t.Fatal(err)
}
defer pool.Close()
defer container.Close(ctx)
```

Parameters:

- `ctx` - Context with timeout
- `name` - Container name prefix (timestamp is appended)
- `verbose` - Enable verbose SQL logging
- `traceFn` - Optional trace function for SQL queries
- `searchPath` - Optional variadic parameter to set schema search path

## Verbose Mode

When running tests with `-v`, SQL queries are logged:

```bash
go test -v ./...
```

The trace function receives all executed SQL with arguments and any errors.
