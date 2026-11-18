# go-pg

PostgreSQL for Go, whose operations map directly onto API calls. You write the SQL; the library binds it and scans the results into your types. Built on [pgx](https://github.com/jackc/pgx), with the same semantics as [go-sqlite](https://github.com/mutablelogic/go-sqlite) for SQLite.

Documentation: <https://pkg.go.dev/github.com/mutablelogic/go-pg>

## Operations map onto API calls

Each operation is an API call. A request type selects the rows, and a response type reads them:

* **Insert** a row, and return it (_POST_ or _PUT_);
* **Update** rows, and optionally return them (_PATCH_);
* **Delete** rows, and optionally return them (_DELETE_);
* **Get** a row (_GET_);
* **List** rows, and count them (_GET_).

So a handler is one call, and `pg.HTTPError` turns its errors into HTTP errors, such as not found or conflict:

```go
// GET /objects/{id}
func (h *Handler) Get(w http.ResponseWriter, r *http.Request, id int64) error {
  var obj MyObject
  if err := h.conn.Get(r.Context(), &obj, MyObject{Id: id}); err != nil {
    return httpresponse.Error(w, pg.HTTPError(err))
  }
  return httpresponse.JSON(w, http.StatusOK, 2, obj)
}

// POST /objects
func (h *Handler) Create(w http.ResponseWriter, r *http.Request) error {
  var req MyObject
  if err := httprequest.Read(r, &req); err != nil {
    return httpresponse.Error(w, err)
  }
  var obj MyObject
  if err := h.conn.Insert(r.Context(), &obj, req); err != nil {
    return httpresponse.Error(w, pg.HTTPError(err))
  }
  return httpresponse.JSON(w, http.StatusCreated, 2, obj)
}
```

A handler for go-sqlite, against SQLite, has the same shape, with `sqlite.HTTPError`.

## SQL first, not an ORM

There's no query builder, no generated SQL, and no mapping of struct tags to tables. Your types return the SQL for each operation, and bind its arguments:

```go
func (obj MyObject) Select(bind *pg.Bind, op pg.Op) (string, error) {
  bind.Set("id", obj.Id)
  switch op {
  case pg.Get:
    return `SELECT id, name FROM mytable WHERE id = @id`, nil
  case pg.Delete:
    return `DELETE FROM mytable WHERE id = @id RETURNING id, name`, nil
  }
  return "", pg.ErrNotImplemented
}
```

* **What runs is what you read.** Queries can be reviewed, and tested in `psql`, as they are.
* **All of PostgreSQL:** CTEs, window functions, JSONB, `RETURNING`, `ON CONFLICT`, `LISTEN` and `NOTIFY`, with nothing to wait for or work around.
* **Queries in files:** keep them in `.sql` files, by name, with `WithQueries`, and `${key}` substitutions for names, such as a schema.
* **Thin types:** a `Selector` binds and returns SQL, a `Reader` scans a row, and a `Writer` binds values. Nothing else needs to know about the database.

## Features

* **Bulk inserts and transactions.**
* **Notify and listen,** and changes published to a broadcaster, with [pgbroadcaster](pgbroadcaster).
* **Tracing and observability.**
* [PostgreSQL Manager](pgmanager/README.md) for server administration, with a REST API, an optional frontend and Prometheus metrics.
* [Task Queue](pgqueue/README.md) for tasks, and tasks at intervals, with PostgreSQL as their backend.
* [Testing utilities](pkg/test/README.md) for integration tests with testcontainers.

## Interfaces

In order to support database operations on go types, those types need to implement one or
more of the following interfaces:

### Selector

```go
type Selector interface {
  // Bind row selection variables, returning the SQL statement required for the operation
  // The operation can be Get, Update, Delete or List
  Select(*Bind, Op) (string, error)
}
```

A type which implements a `Selector` interface can be used to select rows from a table, for get, list, update
and deleting operations.

### Reader

```go
type Reader interface {
  // Scan a row into the receiver
  Scan(Row) error
}
```

A type which implements a `Reader` interface can be used to translate SQL types to the type instance. If multiple
rows are returned, then the `Scan` method is called repeatly until no more rows are returned.

```go
type ListReader interface {
  // Scan a count of returned rows into the receiver
  ScanCount(Row) error
}
```

A type which implements a `ListReader` interface can be used to scan the count of rows returned.

### Writer

```go
type Writer interface {
  // Bind insert parameters, returning the SQL statement required for the insert
  Insert(*Bind) (string, error)

  // Bind update parameters
  Update(*Bind) error
}
```

A type which implements a `Writer` interface can be used to bind object instance variables to SQL parameters.
An example of how to implement an API gateway using this package is shown below.

## Database Server Connection Pool

You can create a connection pool to a database server using the `pg.NewPool` function:

```go
import (
  pg "github.com/mutablelogic/go-pg"
)

func main() {
  pool, err := pg.NewPool(ctx,
    pg.WithHostPort(host, port),
    pg.WithCredentials("postgres", "password"),
    pg.WithDatabase(name),
  )
  if err != nil {
      panic(err)
  }
  defer pool.Close()

  // ...
}
```

The options that can be passed to `pg.NewPool` are:

* `pg.WithURL(string)` - Set connection parameters from a PostgreSQL URL in the format
  `postgres://user:password@host:port/database?sslmode=disable`. Query parameters are
  passed as additional connection options.
* `pg.WithCredentials(string, string)` - Set connection pool username and password.
  If the database name is not set, then the username will be used as the default database name.
* `pg.WithDatabase(string)` - Set the database name for the connection. If the user name is not set,
  then the database name will be used as the user name.
* `pg.WithAddr(string)` - Set the address (host) or (host:port) for the connection.
* `pg.WithHostPort(string, string)` - Set the hostname and port for the connection. If the port
  is not set, then the default port 5432 will be used.
* `pg.WithSSLMode(string)` - Set the SSL connection mode. Valid values are "disable", "allow",
  "prefer", "require", "verify-ca", "verify-full". See
  <https://www.postgresql.org/docs/current/libpq-ssl.html> for more information.
* `pg.WithApplicationName(string)` - Set the application name for the connection.
  This appears in `pg_stat_activity` and helps identify connections.
* `pg.WithSchemaSearchPath(string...)` - Set the schema search path for the connection.
* `pg.WithTrace(pg.TraceFn)` - Set the trace function for the connection pool.
  The signature is `func(ctx context.Context, sql string, args any, err error)`.
* `pg.WithTracer(trace.Tracer)` - Set an OpenTelemetry tracer for the connection pool.
  Spans are created for every query executed through the pool.
* `pg.WithBind(string, any)` - Set a bind variable for the lifetime of the connection.
* `pg.WithPassword(string)` - Override the connection password without affecting the username
  or database name. Useful when the password is supplied separately from the URL.

## Executing Statements

To simply execute a statement, use the `Exec` call:

```go
  if err := pool.Exec(ctx, `CREATE TABLE test (id SERIAL PRIMARY KEY, name TEXT)`); err != nil {
    panic(err)
  }
```

You can use `bind variables` to bind named arguments to a statement using the `With` function.
Within the statement, the following formats are replaced with bound values:

* `${"name"}` - Replace with the value of the named argument "name", double-quoted string
* `${'name'}` - Replace with the value of the named argument "name", single-quoted string
* `${name}` - Replace with the value of the named argument "name", unquoted string
* `$$` - Pass a literal dollar sign
* `@name` - Pass by bound variable parameter

For example,

```go
  var name string
  // ...
  if err := pool.With("table", "test", "name", name).Exec(ctx, `INSERT INTO ${"table"} (name) VALUES (@name)`); err != nil {
    panic(err)
  }
```

This will re-use or create a new database connection from the connection, pool, bind the named arguments, replace
the named arguments in the statement, and execute the statement.

## Implementing Get

If you have a http handler which needs to get a row from a table, you can implement a `Selector` interface.
For example,

```go
type MyObject struct {
  Id int
  Name string
}

// Reader - bind to object
func (obj *MyObject) Scan(row pg.Row) error {
  return row.Scan(&obj.Id, &obj.Name)
}

// Selector - select rows from database
func (obj MyObject) Select(bind *pg.Bind, op pg.Op) (string, error) {
  switch op {
  case pg.Get:
    bind.Set("id", obj.Id)
    return `SELECT id, name FROM mytable WHERE id=@id`, nil
  }
}

// Select the row from the database
func main() {
  // ...
  var obj MyObject
  if err := conn.Get(ctx, &obj, MyObject{ Id: 1 }); err != nil {
    panic(err)
  }
  // ...
}
```

## Implementing List

You may wish to use paging to list rows from a table. The `List` operation is used to
list rows from a table, with offset and limit parameters.
The http handler may look like this:

```go
func ListHandler(w http.ResponseWriter, r *http.Request) {
  var conn pg.Conn

  // ....Set pool....

  // Get up to 10 rows
  var response MyList
  if err := conn.List(ctx, &response, MyListRequest{Offset: 0, Limit: 10}); err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }

  // Write the row to the response - TODO: Add Content-Type header
  json.NewEncoder(w).Encode(response)
}

```

The implementation of MyList and MyListRequest may look like this:

```go
type MyListRequest struct {
  Offset uint64
  Limit uint64
}

type MyList struct {
  Count uint64
  Names []string
}

// Reader - note this needs to be a pointer receiver
func (obj *MyList) Scan(row pg.Row) error {
  var name string
  if err := row.Scan(&name); err != nil {
    return err
  }
  obj.Names = append(obj.Names, name)
  return nil
}

// ListReader - optional interface to scan count of all rows
func (obj *MyList) ScanCount(row pg.Row) error {
  return row.Scan(&obj.Count)
}

// Selector - select rows from database. Use bind variables
// offsetlimit, groupby and orderby to filter the selected rows.
func (obj MyListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
  bind.Set("offsetlimit", fmt.Sprintf("OFFSET %v LIMIT %v",obj.Offset,obj.Limit))
  switch op {
  case pg.List:
    return `SELECT name FROM mytable`, nil
  default:
    return "", fmt.Errorf("unsupported operation: %v", op)
  }
}
```

You can of course use a `WHERE` clause in your query to filter the rows returned from
the table. Always implement the `offsetlimit` as a bind variable.

## Implementing Insert

To insert a row into a table, implement the `Writer` interface:

```go
type MyObject struct {
  Id   int
  Name string
}

// Writer - bind insert parameters
func (obj MyObject) Insert(bind *pg.Bind) (string, error) {
  bind.Set("name", obj.Name)
  return `INSERT INTO mytable (name) VALUES (@name) RETURNING id, name`, nil
}

// Reader - scan the returned row
func (obj *MyObject) Scan(row pg.Row) error {
  return row.Scan(&obj.Id, &obj.Name)
}

// Insert a row
func main() {
  // ...
  obj := MyObject{Name: "hello"}
  if err := conn.Insert(ctx, &obj, obj); err != nil {
    panic(err)
  }
  fmt.Println("Inserted with ID:", obj.Id)
}
```

The `RETURNING` clause allows you to get the inserted row back, including any auto-generated values like serial IDs.

## Implementing Patch

To update rows in a table, implement both `Selector` (to identify rows) and `Writer` (for update values):

```go
type MyObject struct {
  Id   int
  Name string
}

// Selector - identify rows to update
func (obj MyObject) Select(bind *pg.Bind, op pg.Op) (string, error) {
  switch op {
  case pg.Update:
    bind.Set("id", obj.Id)
    return `UPDATE mytable SET name = @name WHERE id = @id RETURNING id, name`, nil
  }
  return "", pg.ErrNotImplemented
}

// Writer - bind update parameters
func (obj MyObject) Update(bind *pg.Bind) error {
  bind.Set("name", obj.Name)
  return nil
}

// Reader - scan the returned row
func (obj *MyObject) Scan(row pg.Row) error {
  return row.Scan(&obj.Id, &obj.Name)
}

// Update a row
func main() {
  // ...
  obj := MyObject{Id: 1, Name: "updated"}
  if err := conn.Update(ctx, &obj, obj, obj); err != nil {
    panic(err)
  }
}
```

## Implementing Delete

To delete rows from a table, implement the `Selector` interface:

```go
type MyObject struct {
  Id   int
  Name string
}

// Selector - identify rows to delete
func (obj MyObject) Select(bind *pg.Bind, op pg.Op) (string, error) {
  switch op {
  case pg.Delete:
    bind.Set("id", obj.Id)
    return `DELETE FROM mytable WHERE id = @id RETURNING id, name`, nil
  }
  return "", pg.ErrNotImplemented
}

// Reader - optionally scan deleted rows
func (obj *MyObject) Scan(row pg.Row) error {
  return row.Scan(&obj.Id, &obj.Name)
}

// Delete a row
func main() {
  // ...
  var deleted MyObject
  if err := conn.Delete(ctx, &deleted, MyObject{Id: 1}); err != nil {
    panic(err)
  }
  fmt.Println("Deleted:", deleted.Name)
}
```

The `RETURNING` clause is optional but useful for confirming what was deleted.

## Transactions

Transactions are executed within a function called `Tx`. For example,

```go
  if err := pool.Tx(ctx, func(tx pg.Conn) error {
    if err := tx.Exec(ctx, `CREATE TABLE test (id SERIAL PRIMARY KEY, name TEXT)`); err != nil {
      return err
    }
    if err := tx.Exec(ctx, `INSERT INTO test (name) VALUES ('hello')`); err != nil {
      return err
    }
    return nil
  }); err != nil {
    panic(err)
  }
```

Any error returned from the function will cause the transaction to be rolled back. If the function returns `nil`, then
the transaction will be committed. Transactions can be nested.

## Notify and Listen

PostgreSQL supports asynchronous notifications via `NOTIFY` and `LISTEN`. The preferred API is `Subscribe`, which acquires a dedicated listener connection from the pool, returns a notification channel, and automatically unsubscribes when the context is cancelled or the pool is closed.

```go
import (
  "context"
  "fmt"

  pg "github.com/mutablelogic/go-pg"
)

// Subscribe to a channel using a pool-backed connection.
notifications, err := pool.Subscribe(ctx, "my_channel")
if err != nil {
  panic(err)
}

for {
  select {
  case <-ctx.Done():
    return
  case n, ok := <-notifications:
    if !ok {
      return
    }
    fmt.Printf("Channel: %s, Payload: %s\n", n.Channel, n.Payload)
  }
}
```

Subscriptions are long-lived and tied to a dedicated PostgreSQL session, so they are only supported on pool-backed connections. Calling `Subscribe` from a transactional or bulk connection returns `pg.ErrNotAvailable`.

`Subscribe` returns setup errors only. After registration, the subscription runs in the background and closes the returned channel if the context is cancelled or if the listener encounters an error such as a dropped connection.

When `pool.Close()` is called, all active subscriptions are cancelled and the pool waits for their listener goroutines to exit and channels to close before returning.

To send a notification from another connection:

```go
if err := pool.Exec(ctx, `NOTIFY my_channel, 'hello world'`); err != nil {
  panic(err)
}
```

The lower-level `Listener` API still exists for compatibility, but `Subscribe` is the recommended interface for new code.

## Schema Support

The package provides convenience functions for managing PostgreSQL schemas:

```go
import pg "github.com/mutablelogic/go-pg"

// Check if a schema exists
exists, err := pg.SchemaExists(ctx, conn, "myschema")

// Create a schema (IF NOT EXISTS)
err := pg.SchemaCreate(ctx, conn, "myschema")

// Drop a schema (IF EXISTS, CASCADE)
err := pg.SchemaDrop(ctx, conn, "myschema")
```

## Error Handling and Tracing

The package provides typed errors for common PostgreSQL conditions. Most query helpers already
normalize driver errors before returning them, but `pg.NormalizeError` is available if you need to
normalize a raw `pgx` or `pgconn` error yourself.

Common checks are:

* `pg.ErrNotFound` for `pgx.ErrNoRows`
* `pg.ErrConflict` and `pg.ErrUniqueViolation` for SQLSTATE `23505`
* `pg.ErrBadParameter` for common input and constraint errors such as foreign key, not null,
  check constraint and invalid text/date formats
* `pg.ErrDatabase` for any PostgreSQL error with a SQLSTATE code

```go
import (
  "errors"
  "log"

  pg "github.com/mutablelogic/go-pg"
)

err := conn.Update(ctx, &obj, req, req)
err = pg.NormalizeError(err)

switch {
case errors.Is(err, pg.ErrNotFound):
  // Row not found
case errors.Is(err, pg.ErrConflict):
  // Duplicate key or other conflict
case errors.Is(err, pg.ErrBadParameter):
  // Invalid user-supplied data
case errors.Is(err, pg.ErrDatabase):
  // Other PostgreSQL error
  log.Printf("sqlstate=%s err=%v", pg.SQLState(err), err)
case err != nil:
  // Non-database error
}
```

If you need the specific PostgreSQL code for logging or translation, use `pg.SQLState(err)`.

```go
import pg "github.com/mutablelogic/go-pg"

if err := conn.Get(ctx, &obj, req); err != nil {
  if errors.Is(err, pg.ErrNotFound) {
    // Row not found
  } else if errors.Is(err, pg.ErrConflict) {
    // Conflict
  } else if errors.Is(err, pg.ErrBadParameter) {
    // Invalid parameter
  } else if errors.Is(err, pg.ErrDatabase) {
    log.Printf("postgres error: sqlstate=%s err=%v", pg.SQLState(err), err)
  } else {
    // Other error
  }
}
```

To enable query tracing, pass a trace function when creating the pool:

```go
pool, err := pg.NewPool(ctx,
  pg.WithHostPort(host, port),
  pg.WithCredentials(user, pass),
  pg.WithTrace(func(ctx context.Context, sql string, args any, err error) {
    if err != nil {
      log.Printf("ERROR: %s: %v", sql, err)
    } else {
      log.Printf("SQL: %s args=%v", sql, args)
    }
  }),
)
```

The trace function is called for every query executed through the connection pool.

## Testing Support

The `pkg/test` package provides utilities for integration testing with PostgreSQL using testcontainers.

See [pkg/test/README.md](pkg/test/README.md) for documentation.

## PostgreSQL Manager

See [pgmanager/README.md](pgmanager/README.md) for documentation.

## Task Queue

See [pgqueue/README.md](pgqueue/README.md) for documentation.
