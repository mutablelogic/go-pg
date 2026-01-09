# go-pg

Postgresql Support for Go, built on top of [pgx](https://github.com/jackc/pgx). This module provides:

* Binding SQL statements to named arguments;
* Support for mapping go structures to SQL tables, and vice versa;
* Easy semantics for Insert, Delete, Update, Get and List operations;
* Bulk insert operations and transactions;
* Support for queries stored in external files with named parameters to improve separation of concerns;
* Support for tracing and observability;
* [PostgreSQL Manager](pkg/manager/README.md) for server administration with REST API, an optional frontend and prometheus metrics;
* [Task Queue](pkg/queue/README.md) for task queue (and interval task) processing using PostgreSQL as a backend;
* [Testing utilities](pkg/test/README.md) for integration testing with testcontainers.

Documentation: <https://pkg.go.dev/github.com/mutablelogic/go-pg>

## Motivation

The package provides a simple way to interact with a Postgresql database from Go, to reduce
the amount of boilerplate code required to interact with the database. The supported operations
align with API calls _POST_, _PUT_, _GET_, _DELETE_ and _PATCH_.

* **Insert** - Insert a row into a table, and return the inserted row (_POST_ or _PUT_);
* **Delete** - Delete one or more rows from a table, and optionally return the deleted rows (_DELETE_);
* **Update** - Update one or more rows in a table, and optionally return the updated rows (_PATCH_);
* **Get** - Get a single row from a table (_GET_);
* **List** - Get a list of rows from a table (_GET_).

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

* `WithURL(string)` - Set connection parameters from a PostgreSQL URL in the format
  `postgres://user:password@host:port/database?sslmode=disable`. Query parameters are
  passed as additional connection options.
* `WithCredentials(string,string)` - Set connection pool username and password.
  If the database name is not set, then the username will be used as the default database name.
* `WithDatabase(string)` - Set the database name for the connection. If the user name is not set,
  then the database name will be used as the user name.
* `WithAddr(string)` - Set the address (host) or (host:port) for the connection
* `WithHostPort(string, string)` - Set the hostname and port for the
  connection. If the port is not set, then the default port 5432 will be used.
* `WithSSLMode( string)` - Set the SSL connection mode. Valid values are
  "disable", "allow", "prefer", "require",  "verify-ca", "verify-full". See
  <https://www.postgresql.org/docs/current/libpq-ssl.html> for more information.
* `pg.WithTrace(pg.TraceFn)` -  Set the trace function for the connection pool.
  The signature of the trace unction is
  `func(ctx context.Context, sql string, args any, err error)`
  and is called for every query executed by the connection pool.
* `pg.WithBind(string,any)` - Set the bind variable to a value the
  the lifetime of the connection.

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
  obj = append(obj, row.String())
  return nil
}

// ListReader - optional interface to scan count of all rows
func (obj MyList) Scan(row pg.Row) error {
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
    return "", fmt.Errorf("Unsupported operation: ",op)
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
  if err := conn.Update(ctx, &obj, obj); err != nil {
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
  if err := pool.Tx(ctx, func(tx pg.Tx) error {
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

PostgreSQL supports asynchronous notifications via `NOTIFY` and `LISTEN`. Use `pg.NewListener` to subscribe to channels:

```go
import pg "github.com/mutablelogic/go-pg"

// Create a listener
listener, err := pg.NewListener(ctx, pool, "my_channel")
if err != nil {
  panic(err)
}
defer listener.Close()

// Listen for notifications
for {
  select {
  case notification := <-listener.C():
    fmt.Printf("Channel: %s, Payload: %s\n", notification.Channel, notification.Payload)
  case <-ctx.Done():
    return
  }
}
```

To send a notification from another connection:

```go
if err := pool.Exec(ctx, `NOTIFY my_channel, 'hello world'`); err != nil {
  panic(err)
}
```

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

## Error Handing and Tracing

The package provides typed errors for common PostgreSQL conditions:

```go
import pg "github.com/mutablelogic/go-pg"

if err := conn.Get(ctx, &obj, req); err != nil {
  if errors.Is(err, pg.ErrNotFound) {
    // Row not found
  } else if errors.Is(err, pg.ErrDuplicate) {
    // Unique constraint violation
  } else if errors.Is(err, pg.ErrBadParameter) {
    // Invalid parameter
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

The `pkg/manager` package provides a comprehensive API for managing PostgreSQL server resources including roles, databases, schemas, tables, connections, replication slots, and more. It includes a REST API with Prometheus metrics.

See [pkg/manager/README.md](pkg/manager/README.md) for documentation.
