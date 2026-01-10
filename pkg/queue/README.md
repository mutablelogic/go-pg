# PostgreSQL Task Queue

The `queue` package provides a PostgreSQL-backed task queue with support for delayed tasks, retries with exponential backoff, and periodic tickers. It enables reliable background job processing through both a direct Go API and HTTP interfaces.

To test the package:

```bash
git clone github.com/mutablelogic/go-pg
make tests
```

You'll need to have docker installed in order to run the integration tests, which will create a PostgreSQL server in a container. There is a command line client included for testing:

```bash
git clone github.com/mutablelogic/go-pg
make cmd/pgqueue
```

This places a `pgqueue` binary in the `build` folder which you can use as a server or client. To run the server on localhost, port 8080:

```bash
build/pgqueue run postgres://localhost:5432/postgres
```

To use the client:

```bash
build/pgqueue queues
build/pgqueue tasks --status=new
```

Run `build/pgqueue --help` for more information and to understand the available commands and settings.

## Architecture

The package is organized into four main components:

### Manager (this package folder)

The core component that provides direct access to queue operations. It wraps a connection pool and exposes methods for managing queues, tasks, and tickers.

```go
import "github.com/mutablelogic/go-pg/pkg/queue"

// Create a manager with namespace isolation
mgr, err := queue.New(ctx, pool, "myapp")
```

### Schema (`schema/`)

Defines all data types, request/response structures, and SQL queries. Each resource type has its own file containing:

- **Structs** - Go types representing queue objects (`Queue`, `Task`, `Ticker`)
- **Meta types** - Parameters for creating/updating resources
- **SQL generation** - Methods that produce parameterized SQL queries

### HTTP Handler (`httphandler/`)

Provides REST API endpoints for all queue operations. Register handlers with an `http.ServeMux`:

```go
import "github.com/mutablelogic/go-pg/pkg/queue/httphandler"

httphandler.RegisterBackendHandlers(mux, "/api", mgr)
```

### HTTP Client (`httpclient/`)

A typed client for consuming the REST API from Go applications:

```go
import "github.com/mutablelogic/go-pg/pkg/queue/httpclient"

client, err := httpclient.New("http://localhost:8080/api")
queues, err := client.ListQueues(ctx)
```

## Core Concepts

### Queues

Queues hold tasks and define retry behavior:

```go
ttl := 24 * time.Hour
retries := uint64(3)
retryDelay := time.Minute

queue, err := mgr.RegisterQueue(ctx, schema.QueueMeta{
    Queue:      "emails",
    TTL:        &ttl,        // Task expiration
    Retries:    &retries,    // Max retry attempts
    RetryDelay: &retryDelay, // Base delay (exponential backoff)
})
```

### Tasks

Tasks progress through a lifecycle:

| Status | Description |
|--------|-------------|
| **new** | Newly created, waiting to be processed |
| **retry** | Failed but has retries remaining, waiting for backoff delay |
| **retained** | Locked by a worker for processing |
| **released** | Finished successfully |
| **failed** | Exhausted all retries |
| **expired** | TTL exceeded before completion |

Tasks with status `released`, `failed`, or `expired` are automatically cleaned up when the queue's TTL expires.

Create and process tasks:

```go
// Create a task
task, err := mgr.CreateTask(ctx, "emails", schema.TaskMeta{
    Payload: map[string]any{"to": "user@example.com"},
})

// Create a delayed task
delayedAt := time.Now().Add(time.Hour)
task, err := mgr.CreateTask(ctx, "emails", schema.TaskMeta{
    Payload:   map[string]any{"to": "user@example.com"},
    DelayedAt: &delayedAt,
})

// Worker: get next available task from specific queue
task, err := mgr.NextTask(ctx, "worker-1", "emails")

// Worker: get next available task from any queue
task, err := mgr.NextTask(ctx, "worker-1")

// Worker: get next available task from multiple queues
task, err := mgr.NextTask(ctx, "worker-1", "emails", "notifications")

// Release task (success)
mgr.ReleaseTask(ctx, task.Id, true, nil, &status)

// Release task (failure - will retry with backoff)
mgr.ReleaseTask(ctx, task.Id, false, errPayload, &status)
```

### Tickers

Tickers generate tasks on a schedule:

```go
period := time.Hour
ticker, err := mgr.RegisterTicker(ctx, schema.TickerMeta{
    Ticker:  "hourly-report",
    Queue:   "reports",
    Period:  &period,
    Payload: map[string]any{"type": "hourly"},
})

// Process matured tickers
mgr.RunTickerLoop(ctx, func(ticker *schema.Ticker) error {
    _, err := mgr.CreateTask(ctx, ticker.Queue, schema.TaskMeta{
        Payload: ticker.Payload,
    })
    return err
})
```

### Namespaces

Each manager operates within a namespace for multi-tenant isolation:

```go
appMgr, _ := queue.New(ctx, pool, "app")
adminMgr, _ := queue.New(ctx, pool, "admin")
// Queues and tasks are completely independent
```

A special `pgqueue` system namespace is automatically created and used for internal operations like expired task cleanup. This namespace should not be used by applications.

## REST API Endpoints

All endpoints are prefixed with the configured path (e.g., `/api`):

### Namespace

| Method | Path | Description |
|--------|------|-------------|
| GET | `{prefix}/namespace` | List all namespaces |

### Queue

| Method | Path | Description |
|--------|------|-------------|
| GET | `{prefix}/queue` | List queues |
| POST | `{prefix}/queue` | Create queue |
| GET | `{prefix}/queue/{name}` | Get queue by name |
| PATCH | `{prefix}/queue/{name}` | Update queue |
| DELETE | `{prefix}/queue/{name}` | Delete queue |

### Task

| Method | Path | Description |
|--------|------|-------------|
| GET | `{prefix}/task` | List tasks (filter: `?queue=`, `?status=`, `?offset=`, `?limit=`) |
| POST | `{prefix}/task` | Create task |
| PUT | `{prefix}/task` | Retain next task from any queue (requires `?worker=`) |
| PUT | `{prefix}/task/{queue}` | Retain next task from specific queue (requires `?worker=`) |
| PATCH | `{prefix}/task/{id}` | Release task with result (`{"fail": bool, "result": any}`) |

### Ticker

| Method | Path | Description |
|--------|------|-------------|
| GET | `{prefix}/ticker` | List tickers |
| POST | `{prefix}/ticker` | Create ticker |
| GET | `{prefix}/ticker/{name}` | Get ticker by name |
| PATCH | `{prefix}/ticker/{name}` | Update ticker |
| DELETE | `{prefix}/ticker/{name}` | Delete ticker |
| GET | `{prefix}/ticker/next` | SSE stream of matured tickers |

### Metrics

| Method | Path | Description |
|--------|------|-------------|
| GET | `{prefix}/metrics` | Prometheus metrics |

Exposes the `queue_tasks` gauge metric with labels for namespace, queue, and status.

## CLI Commands

```bash
# Namespace operations
pgqueue namespaces                 # List namespaces

# Queue operations
pgqueue queues                     # List queues
pgqueue queue myqueue              # Get queue details
pgqueue create-queue myqueue       # Create queue
pgqueue update-queue myqueue       # Update queue
pgqueue delete-queue myqueue       # Delete queue

# Task operations
pgqueue tasks                      # List all tasks
pgqueue tasks --queue=myqueue      # Filter by queue
pgqueue tasks --status=new         # Filter by status
pgqueue create-task myqueue        # Create task
pgqueue retain-task --worker=worker1 myqueue  # Retain next task from specific queue
pgqueue retain-task --worker=worker1          # Retain next task from any queue
pgqueue retain-task --worker=worker1 queue1 queue2  # Retain from multiple queues
pgqueue complete-task 123          # Release task (success)
pgqueue complete-task 123 --error '{"msg":"failed"}'  # Release task (failure)

# Ticker operations
pgqueue tickers                    # List tickers
pgqueue ticker myticker            # Get ticker details
pgqueue create-ticker myticker     # Create ticker
pgqueue update-ticker myticker     # Update ticker
pgqueue delete-ticker myticker     # Delete ticker
pgqueue next-ticker                # Stream matured tickers (SSE)

# Server
pgqueue run postgres://...         # Run HTTP server
```

## Dependencies

- `github.com/mutablelogic/go-pg` - PostgreSQL connection pool
- `github.com/mutablelogic/go-server` - HTTP utilities
- `github.com/mutablelogic/go-client` - HTTP client
