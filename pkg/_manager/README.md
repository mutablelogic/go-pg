# PostgreSQL Manager

The `manager` package provides an API for managing PostgreSQL server resources. It enables introspection and management of databases, roles, schemas, tables, and other PostgreSQL objects through both a direct Go API and HTTP interfaces.
There are unit and integration tests included.

To test the package:

```bash
git clone github.com/mutablelogic/go-pg
make tests
```

You'll need to have docker installed in order to run the integration tests, which will create a PostgreSQL server in a container. There is a command line client included for testing:

```bash
git clone github.com/mutablelogic/go-pg
make cmd/pgmanager
```

This places a binary in the `build` folder which you can use as a server or client. To run the server
on localhost, port 8080:

```bash
build/pgmanager run postgres://localhost:5432/postgres
```

To use the client:

```bash
build/pgmanager databases
```

Run `build/pgmanager --help` for more information.

## Architecture

The package is organized into four main components:

### Manager (this package folder)

The core component that provides direct access to PostgreSQL management functions. It wraps a connection pool and exposes methods for querying and managing server resources.

```go
import "github.com/mutablelogic/go-pg/pkg/manager"

// Create a manager from an existing connection pool
mgr, err := manager.New(ctx, conn)
```

Documentation for all manager methods can be found [here](https://pkg.go.dev/github.com/mutablelogic/go-pg/pkg/manager).

### Schema (`schema/`)

Defines all data types, request/response structures, and SQL queries for PostgreSQL resources. Each resource type has its own file containing:

- **Structs** - Go types representing PostgreSQL objects (e.g., `Role`, `Database`, `Schema`)
- **List requests** - Parameters for filtering and pagination
- **SQL generation** - Methods that produce parameterized SQL queries

### HTTP Handler (`httphandler/`)

Provides REST API endpoints for all management operations. Register handlers with an `http.ServeMux`:

```go
import "github.com/mutablelogic/go-pg/pkg/manager/httphandler"

httphandler.RegisterHandlers(mux, "/api/v1", mgr)
```

Includes a Prometheus metrics endpoint at `/api/v1/metrics` exposing:

- Connection counts by database and state
- Database and tablespace sizes
- Table and index sizes
- Dead tuple ratios for vacuum monitoring
- Replication slot status and lag

### HTTP Client (`httpclient/`)

A typed client for consuming the REST API from Go applications:

```go
import "github.com/mutablelogic/go-pg/pkg/manager/httpclient"

client, err := httpclient.New("http://localhost:8080/api/v1")
roles, err := client.ListRoles(ctx)
```

## Managed Resources

| Resource | Description |
|----------|-------------|
| **Roles** | Database users and groups with their attributes and memberships |
| **Databases** | Database instances with size, owner, encoding, and connection settings |
| **Schemas** | Namespaces within databases containing tables and other objects |
| **Objects** | Tables, views, indexes, sequences, and other database objects |
| **Tablespaces** | Storage locations for database files |
| **Extensions** | PostgreSQL extensions installed on the server |
| **Connections** | Active database connections with state and query information |
| **Settings** | Server configuration parameters |
| **Statements** | Query statistics from `pg_stat_statements` (when available) |
| **Replication Slots** | Logical and physical replication slots with lag metrics |

## API Patterns

All list operations follow a consistent pattern:

```go
// Request with optional filtering and pagination
req := schema.RoleListRequest{
    OffsetLimit: pg.OffsetLimit{
        Offset: 0,
        Limit:  types.Uint64Ptr(100),
    },
    Name: types.StringPtr("admin%"),  // LIKE pattern
}

// Response includes total count and body
list, err := mgr.ListRoles(ctx, req)
fmt.Printf("Total: %d, Returned: %d\n", list.Count, len(list.Body))
```

## REST API Endpoints

All endpoints are prefixed with the configured path (e.g., `/api/v1`):

| Method | Path | Description |
|--------|------|-------------|
| GET | `/roles` | List roles |
| GET | `/roles/{name}` | Get role by name |
| GET | `/databases` | List databases |
| GET | `/databases/{name}` | Get database by name |
| GET | `/schemas` | List schemas |
| GET | `/objects` | List objects (tables, views, indexes, etc.) |
| GET | `/tablespaces` | List tablespaces |
| GET | `/extensions` | List extensions |
| GET | `/connections` | List active connections |
| GET | `/settings` | List server settings |
| GET | `/statements` | List statement statistics |
| GET | `/replicationslots` | List replication slots |
| GET | `/metrics` | Prometheus metrics |

Query parameters support filtering and pagination:

- `offset` - Skip N results
- `limit` - Maximum results to return
- Resource-specific filters (e.g., `database`, `schema`, `type`)

## Dependencies

- `github.com/mutablelogic/go-pg` - PostgreSQL connection pool
- `github.com/mutablelogic/go-server` - HTTP utilities
- `github.com/prometheus/client_golang` - Prometheus metrics
