-- pgqueue.queue 
CREATE TABLE IF NOT EXISTS ${"schema"}."queue" (
    "queue"         TEXT NOT NULL,
    "ttl"           INTERVAL NOT NULL DEFAULT INTERVAL '1 hour',
    "retries"       INTEGER NOT NULL DEFAULT 3 CHECK ("retries" >= 0),
    "retry_delay"   INTERVAL NOT NULL DEFAULT INTERVAL '2 minute',
    "concurrency"   INTEGER NOT NULL DEFAULT 0 CHECK ("concurrency" >= 0), -- 0 = unlimited
    "dedupe"        BOOLEAN NOT NULL DEFAULT false, -- true = a task is not inserted while the same task is pending
    PRIMARY KEY ("queue")
);

ALTER TABLE ${"schema"}."queue"
    ADD COLUMN IF NOT EXISTS "dedupe" BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE ${"schema"}."queue"
    ALTER COLUMN "ttl" SET DEFAULT INTERVAL '1 hour';

UPDATE ${"schema"}."queue"
SET "ttl" = INTERVAL '1 hour'
WHERE "ttl" IS NULL;

ALTER TABLE ${"schema"}."queue"
    ALTER COLUMN "ttl" SET NOT NULL;

-- pgqueue.task_status_type
DO $$ BEGIN
    CREATE TYPE ${"schema"}."task_status" AS ENUM(
        'new',      -- ready to run
        'running',  -- currently locked by a worker
        'retry',    -- waiting for retry delay before next attempt
        'expired',  -- dies_at exceeded before completion
        'failed',   -- exhausted all retries
        'done'      -- completed successfully
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- pgqueue.task_hash_func
-- The hash of a task's queue and payload, to find duplicate tasks. Object
-- fields which are null are left out, so a null field is the same as a missing
-- one. It's immutable, as a generated column needs, since the payload is
-- always converted to UTF8, though convert_to is only stable.
CREATE OR REPLACE FUNCTION ${"schema"}.task_hash(q TEXT, p JSONB) RETURNS BYTEA AS $$
    SELECT sha256(convert_to(q || ':' || jsonb_strip_nulls(p)::TEXT, 'UTF8'))
$$ LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

-- pgqueue.task
CREATE TABLE IF NOT EXISTS ${"schema"}."task" (
    "id"                BIGINT GENERATED ALWAYS AS IDENTITY,
    "queue"             TEXT NOT NULL,
    "payload"           JSONB NOT NULL DEFAULT '{}',
    "result"            JSONB NOT NULL DEFAULT 'null',
    "worker"            TEXT,
    "created_at"        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    "delayed_at"        TIMESTAMPTZ,
    "started_at"        TIMESTAMPTZ,
    "finished_at"       TIMESTAMPTZ,
    "dies_at"           TIMESTAMPTZ,
    "retries"           INTEGER NOT NULL CHECK ("retries" >= 0),
    "initial_retries"   INTEGER NOT NULL CHECK ("initial_retries" >= 0),
    "hash"              BYTEA GENERATED ALWAYS AS (${"schema"}.task_hash("queue", "payload")) STORED, -- hash of the queue and payload, to find duplicate tasks
    PRIMARY KEY ("id"),
    FOREIGN KEY ("queue") REFERENCES ${"schema"}."queue" ("queue") ON DELETE CASCADE
) PARTITION BY RANGE (id);

ALTER TABLE ${"schema"}."task"
    ADD COLUMN IF NOT EXISTS "hash" BYTEA GENERATED ALWAYS AS (${"schema"}.task_hash("queue", "payload")) STORED;

-- pgqueue.queue_status_index
-- Covers 'new' and 'retry' states
CREATE INDEX IF NOT EXISTS "task_queue_status_idx"
    ON ${"schema"}."task" ("queue", "delayed_at")
    WHERE "started_at" IS NULL 
      AND "finished_at" IS NULL 
      AND "retries" > 0;

-- pgqueue.task_hash_index
-- Covers finding a pending task which is the same as an inserted task
CREATE INDEX IF NOT EXISTS "task_hash_idx"
    ON ${"schema"}."task" ("hash")
    WHERE "started_at" IS NULL
      AND "finished_at" IS NULL
      AND "retries" > 0;

-- pgqueue.worker_index
CREATE INDEX IF NOT EXISTS "task_worker_idx"
    ON ${"schema"}."task" ("worker")
    WHERE "started_at" IS NOT NULL 
      AND "finished_at" IS NULL;


-- pgqueue.ticker
CREATE TABLE IF NOT EXISTS ${"schema"}."ticker" (
    "ticker"        TEXT NOT NULL PRIMARY KEY,
    "payload"       JSONB NOT NULL DEFAULT '{}',
    "interval"      INTERVAL DEFAULT INTERVAL '1 minute',
    "last_at"       TIMESTAMPTZ                       -- when it last fired (NULL = never)
);

-- pgqueue.ticker_queue_index
CREATE INDEX IF NOT EXISTS "idx_ticker_next"
    ON ${"schema"}."ticker" ("last_at" NULLS FIRST) 
    WHERE "interval" IS NOT NULL;

-- pgqueue.queue_task_status_func
CREATE OR REPLACE FUNCTION ${"schema"}.queue_task_status(
    dies_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    retries INTEGER,
    initial_retries INTEGER
) RETURNS ${"schema"}.task_status AS $$
    SELECT CASE
        WHEN started_at IS NOT NULL AND finished_at IS NOT NULL                THEN 'done'::${"schema"}.task_status
        WHEN started_at IS NOT NULL AND finished_at IS NULL AND dies_at IS NOT NULL AND dies_at < NOW() THEN 'expired'::${"schema"}.task_status
        WHEN started_at IS NULL AND finished_at IS NULL AND retries = 0        THEN 'failed'::${"schema"}.task_status
        WHEN started_at IS NULL AND finished_at IS NULL AND retries = initial_retries
                                                                            THEN 'new'::${"schema"}.task_status
        WHEN started_at IS NULL AND finished_at IS NULL                        THEN 'retry'::${"schema"}.task_status
        WHEN started_at IS NOT NULL AND finished_at IS NULL                    THEN 'running'::${"schema"}.task_status
        ELSE NULL
    END AS "status";
$$ LANGUAGE SQL STABLE;

-- pgqueue.queue_insert_task_func
-- Inserts a task into the queue, with the queue's retries, and returns its id.
-- A delay in the past is now, and no delay runs the task as soon as possible.
CREATE OR REPLACE FUNCTION ${"schema"}.queue_insert_task(q TEXT, p JSONB, delayed_at TIMESTAMPTZ, retries INTEGER) RETURNS BIGINT AS $$
    INSERT INTO ${"schema"}."task" ("queue", "payload", "delayed_at", "retries", "initial_retries")
    VALUES (
        q, p, CASE
            WHEN delayed_at IS NULL THEN NULL
            WHEN delayed_at < NOW() THEN NOW()
            ELSE delayed_at
        END, retries, retries
    )
    RETURNING "id";
$$ LANGUAGE SQL;

-- pgqueue.queue_insert_func
-- Inserts a task into the queue, and returns its id. When the queue
-- deduplicates tasks, a pending task which is the same is returned instead.
CREATE OR REPLACE FUNCTION ${"schema"}.queue_insert(q TEXT, p JSONB, delayed_at TIMESTAMPTZ) RETURNS BIGINT AS $$
DECLARE
    v_retries       INTEGER;
    v_dedupe        BOOLEAN;
BEGIN
    -- Get queue defaults, raise if queue doesn't exist
    SELECT
        "retries", "dedupe"
    INTO STRICT
        v_retries, v_dedupe
    FROM
        ${"schema"}."queue"
    WHERE
        "queue" = q;

    -- Insert the task, or return the same pending task
    IF v_dedupe THEN
        RETURN ${"schema"}.queue_insert_dedupe(q, p, delayed_at);
    END IF;
    RETURN ${"schema"}.queue_insert_task(q, p, delayed_at, v_retries);
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'queue not found: %', q;
END;
$$ LANGUAGE PLPGSQL;

-- pgqueue.queue_insert_dedupe_func
-- Returns the id of a pending task in the queue which is the same as the task,
-- with the same queue and payload, or else inserts the task and returns its id.
-- A pending task hasn't started or finished, and has retries left, so a
-- running task isn't the same, and a change while it runs queues another. A
-- new task, which hasn't been tried, runs at the earlier of the two delays;
-- a task waiting to be retried keeps its backoff. Inserts of the same task are
-- serialized with a lock on its hash until the transaction ends, as the task
-- table is partitioned by id, so a unique index can't prevent duplicates.
CREATE OR REPLACE FUNCTION ${"schema"}.queue_insert_dedupe(q TEXT, p JSONB, delayed_at TIMESTAMPTZ) RETURNS BIGINT AS $$
DECLARE
    v_hash          BYTEA := ${"schema"}.task_hash(q, p);
    v_delayed_at    TIMESTAMPTZ := CASE
        WHEN delayed_at IS NULL THEN NULL
        WHEN delayed_at < NOW() THEN NOW()
        ELSE delayed_at
    END;
    v_retries       INTEGER;
    v_id            BIGINT;
    v_existing_at   TIMESTAMPTZ;
    v_untried       BOOLEAN;
BEGIN
    -- Get queue defaults, raise if queue doesn't exist
    SELECT
        "retries"
    INTO STRICT
        v_retries
    FROM
        ${"schema"}."queue"
    WHERE
        "queue" = q;

    -- Serialize inserts of the same task, with the first eight bytes of the hash
    PERFORM pg_advisory_xact_lock(('x' || encode(substring(v_hash FROM 1 FOR 8), 'hex'))::BIT(64)::BIGINT);

    -- Find a pending task which is the same. The columns are qualified, as
    -- the delayed_at argument has the name of a column.
    SELECT
        T."id", T."delayed_at", T."retries" = T."initial_retries"
    INTO
        v_id, v_existing_at, v_untried
    FROM
        ${"schema"}."task" T
    WHERE
        T."hash" = v_hash
    AND
        T."queue" = q
    AND
        T."started_at" IS NULL AND T."finished_at" IS NULL AND T."retries" > 0
    ORDER BY
        T."id"
    LIMIT 1
    FOR UPDATE;

    -- Insert the task when there's none
    IF NOT FOUND THEN
        RETURN ${"schema"}.queue_insert_task(q, p, delayed_at, v_retries);
    END IF;

    -- Run a new task at the earlier of the two delays, where no delay is the
    -- earliest, and notify the workers, as it may be ready to run now
    IF v_untried AND COALESCE(v_delayed_at, '-infinity') < COALESCE(v_existing_at, '-infinity') THEN
        UPDATE ${"schema"}."task" T SET "delayed_at" = v_delayed_at WHERE T."id" = v_id;
        PERFORM pg_notify(${'channel'}, json_build_object(
            'schema', ${'schema'},
            'queue', q
        )::TEXT);
    END IF;

    RETURN v_id;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'queue not found: %', q;
END;
$$ LANGUAGE PLPGSQL;

-- pgqueue.queue_notify_func
CREATE OR REPLACE FUNCTION ${"schema"}.queue_notify() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(${'channel'}, json_build_object(
        'schema', ${'schema'},
        'queue', NEW."queue"
    )::TEXT);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- pgqueue.queue_insert_trigger
CREATE OR REPLACE TRIGGER queue_insert_trigger
    AFTER INSERT ON ${"schema"}."task"
    FOR EACH ROW EXECUTE FUNCTION ${"schema"}.queue_notify();

-- pgqueue.queue_lock_func
CREATE OR REPLACE FUNCTION ${"schema"}.queue_lock(q TEXT[], w TEXT) RETURNS BIGINT AS $$
WITH selected AS (
    SELECT
        t."id",
        queue."ttl"
    FROM
        ${"schema"}."task" t
    JOIN
        ${"schema"}."queue" queue
    ON
        queue."queue" = t."queue"
    LEFT JOIN (
        SELECT
            "queue",
            COUNT(*) AS "retained"
        FROM
            ${"schema"}."task"
        WHERE
            "started_at" IS NOT NULL
        AND
            "finished_at" IS NULL
        AND
            ("dies_at" IS NULL OR "dies_at" >= NOW())
        GROUP BY
            "queue"
    ) retained
    ON
        retained."queue" = t."queue"
    WHERE
        (CARDINALITY(q) = 0 OR t."queue" = ANY(q))
    AND
        (
            (t."started_at" IS NULL AND t."finished_at" IS NULL)
            OR
            (t."started_at" IS NOT NULL AND t."finished_at" IS NULL AND t."dies_at" IS NOT NULL AND t."dies_at" < NOW())
        )
    AND
        (t."delayed_at" IS NULL OR t."delayed_at" <= NOW())
    AND
        t."retries" > 0
    AND
        (queue."concurrency" = 0 OR COALESCE(retained."retained", 0) < queue."concurrency")
    ORDER BY
        t."created_at"
    FOR UPDATE OF t SKIP LOCKED LIMIT 1
)
UPDATE ${"schema"}."task" SET
    "started_at" = NOW(),
    "worker" = w,
    "result" = 'null',
    "dies_at" = NOW() + selected."ttl"
FROM selected
WHERE ${"schema"}."task"."id" = selected."id"
RETURNING ${"schema"}."task"."id";
$$ LANGUAGE SQL;

-- pgqueue.queue_unlock_func
CREATE OR REPLACE FUNCTION ${"schema"}.queue_unlock(tid BIGINT, r JSONB) RETURNS BIGINT AS $$
    UPDATE ${"schema"}."task" SET
        "finished_at" = NOW(),
        "result" = r,
        "dies_at" = NULL
    WHERE
        "id" = tid
    AND
        "started_at" IS NOT NULL AND "finished_at" IS NULL
    RETURNING
        "id";
$$ LANGUAGE SQL;

-- pgqueue.queue_backoff_func
CREATE OR REPLACE FUNCTION ${"schema"}.queue_backoff(tid BIGINT) RETURNS TIMESTAMPTZ AS $$
    SELECT CASE
        WHEN T."retries" = 0 THEN NULL
        ELSE NOW() + Q."retry_delay" * (POW(T."initial_retries" - T."retries" + 1, 2))
    END AS "delayed_at"
    FROM
        ${"schema"}."task" T
    JOIN
        ${"schema"}."queue" Q
    ON
        T."queue" = Q."queue"
    WHERE
        T."id" = tid;
$$ LANGUAGE SQL STABLE;

-- pgqueue.queue_fail_func
CREATE OR REPLACE FUNCTION ${"schema"}.queue_fail(tid BIGINT, r JSONB) RETURNS BIGINT AS $$
    UPDATE ${"schema"}."task" SET
        "retries" = "retries" - 1,
        "result" = r,
        "started_at" = NULL,
        "finished_at" = NULL,
        "delayed_at" = CASE
            -- The last failure is when the task failed, which is when its
            -- retention starts, rather than a backoff
            WHEN "retries" = 1 THEN NOW()
            ELSE ${"schema"}.queue_backoff(tid)
        END,
        "dies_at" = NULL
    WHERE
        "id" = tid
    AND
        "retries" > 0
    AND
        "started_at" IS NOT NULL AND "finished_at" IS NULL
    RETURNING
        "id";
$$ LANGUAGE SQL;

-- pgqueue.queue_clean_func
-- Deletes up to 100 of the queue's finished tasks, which are done, failed or
-- expired, and finished longer ago than the retention period, oldest first,
-- and returns them. A task's finish is when it was done, or expired, or else
-- when it was last delayed, which is when it failed, or created.
-- The function without a retention period, from before, is dropped.
DROP FUNCTION IF EXISTS ${"schema"}."queue_clean"(TEXT);
CREATE OR REPLACE FUNCTION ${"schema"}."queue_clean"(q TEXT, retention INTERVAL) RETURNS TABLE (
    "id"            BIGINT,
    "queue"         TEXT,
    "payload"       JSONB,
    "result"        JSONB,
    "worker"        TEXT,
    "created_at"    TIMESTAMPTZ,
    "delayed_at"    TIMESTAMPTZ,
    "started_at"    TIMESTAMPTZ,
    "finished_at"   TIMESTAMPTZ,
    "dies_at"       TIMESTAMPTZ,
    "retries"       INTEGER
) AS $$
    DELETE FROM
        ${"schema"}."task"
    WHERE
        "id" IN (
            WITH sq AS (
                SELECT
                    "id",
                    COALESCE("finished_at", "dies_at", "delayed_at", "created_at") AS "finished",
                    ${"schema"}.queue_task_status(
                        "dies_at",
                        "started_at",
                        "finished_at",
                        "retries",
                        "initial_retries"
                    ) AS "status"
                FROM
                    ${"schema"}."task"
                WHERE
                    "queue" = q
            ) SELECT
                "id"
            FROM
                sq
            WHERE
                "status" IN ('expired'::${"schema"}.task_status, 'done'::${"schema"}.task_status, 'failed'::${"schema"}.task_status)
            AND
                "finished" < NOW() - retention
            ORDER BY
                "finished"
            LIMIT
                100
        )
    RETURNING
        "id", "queue", "payload", "result", "worker", "created_at", "delayed_at", "started_at", "finished_at", "dies_at", "retries";
$$ LANGUAGE SQL;

-- pgqueue.queue_status_view
CREATE OR REPLACE VIEW ${"schema"}."queue_status" AS
SELECT
    Q."queue",
    S."status",
    COUNT(T."id") AS "count"
FROM
    ${"schema"}."queue" Q
CROSS JOIN
    (SELECT UNNEST(enum_range(NULL::${"schema"}.task_status)) AS "status") S
LEFT JOIN
    (
        SELECT
            "id",
            "queue",
            ${"schema"}.queue_task_status(
                "dies_at",
                "started_at",
                "finished_at",
                "retries",
                "initial_retries"
            ) AS "status"
        FROM
            ${"schema"}."task"
    ) T
ON
    S."status" = T."status" AND Q."queue" = T."queue"
GROUP BY
    1, 2;

-- pgqueue.ticker_next_func
-- Returns the ticker, of the tickers with the names, which is most overdue,
-- and marks it as run now. A ticker without a positive interval, which would
-- always be due, isn't returned. Only the tickers a process has callbacks for are
-- returned, so a process doesn't claim the tickers of another process sharing
-- the database. The function without names, from before, is dropped.
DROP FUNCTION IF EXISTS ${"schema"}.ticker_next();
CREATE OR REPLACE FUNCTION ${"schema"}.ticker_next(names TEXT[]) RETURNS TABLE (
    "ticker"    TEXT,
    "interval"  INTERVAL,
    "payload"   JSONB,
    "last_at"   TIMESTAMPTZ
) AS $$
    UPDATE
        ${"schema"}."ticker"
    SET
        "last_at" = NOW()
    WHERE
        "ticker" = (
            SELECT "ticker"
            FROM ${"schema"}."ticker"
            WHERE
                "ticker" = ANY(names)
            AND
                "interval" > INTERVAL '0'
            AND
                ("last_at" IS NULL OR "last_at" + "interval" <= NOW())
            ORDER BY
                "last_at" NULLS FIRST
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
    RETURNING
        "ticker", "interval", "payload", "last_at";
$$ LANGUAGE SQL;
