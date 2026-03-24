# PostgreSQL Broadcaster

The broadcaster package wraps PostgreSQL `LISTEN` and `NOTIFY` and fans decoded JSON notifications out to multiple Go callbacks.

The current implementation subscribes to a PostgreSQL channel via `pg.Conn.Subscribe`, decodes each payload as JSON, and delivers it to every active subscriber until either the subscriber context or broadcaster context is canceled.

The package decodes each payload into `broadcaster.ChangeNotification`, so the SQL payload needs to look like this:

```json
{"schema":"auth","table":"user","action":"INSERT"}
```

## PostgreSQL Trigger Function

One practical way to drive the broadcaster is with a trigger function that emits a JSON payload whenever an `INSERT`, `UPDATE`, `TRUNCATE`, or `DELETE` occurs on a table.

```sql
CREATE OR REPLACE FUNCTION app.notify_table()
RETURNS trigger AS $$
DECLARE
    lock_id BIGINT;
BEGIN
    lock_id := hashtextextended(TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME, 0);
    IF pg_try_advisory_xact_lock(lock_id) THEN
        PERFORM pg_notify(
            'backend.table_change',
            json_build_object(
                'schema', TG_TABLE_SCHEMA,
                'table', TG_TABLE_NAME,
                'action', TG_OP
            )::text
        );
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
```

Attach it to a table with a statement-level trigger:

```sql
DROP TRIGGER IF EXISTS user_table_changes_notify ON app_user;

CREATE TRIGGER user_table_changes_notify
AFTER INSERT OR UPDATE OR TRUNCATE OR DELETE ON app_user
FOR EACH STATEMENT
EXECUTE FUNCTION app.notify_table();
```

The advisory lock suppresses repeated notifications for the same table within a transaction, which is useful when a single statement touches multiple rows.

## Go Example

```go
package main

import (
    "context"
    "log"
    "os/signal"
    "syscall"

    broadcaster "github.com/mutablelogic/go-pg/pkg/broadcaster"
    pg "github.com/mutablelogic/go-pg"
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    pool, err := pg.NewPool(ctx,
        pg.WithURL("postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()

    b, err := broadcaster.NewBroadcaster(pool, "backend.table_change")
    if err != nil {
        log.Fatal(err)
    }
    defer b.Close()

    err = b.Subscribe(ctx, func(change broadcaster.ChangeNotification) {
        log.Printf("table change: schema=%s table=%s action=%s", change.Schema, change.Table, change.Action)
    })
    if err != nil {
        log.Fatal(err)
    }

    <-ctx.Done()
}
```

## Notes

- Notifications with invalid JSON payloads are ignored.
- Each subscriber gets its own buffered channel so slow callbacks do not immediately block the PostgreSQL listener.
- `Close` cancels the listener and waits for subscriber goroutines to exit.
- If you want a different payload shape, keep the same `LISTEN`/`NOTIFY` pattern and adapt the JSON type decoded by the broadcaster package.
