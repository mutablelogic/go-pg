-- pgqueue.queue 
CREATE TABLE IF NOT EXISTS ${"schema"}."queue" (
    "queue"         TEXT NOT NULL,
    "ttl"           INTERVAL NOT NULL DEFAULT INTERVAL '1 hour',
    "retries"       INTEGER NOT NULL DEFAULT 3 CHECK ("retries" >= 0),
    "retry_delay"   INTERVAL NOT NULL DEFAULT INTERVAL '2 minute',
    "concurrency"   INTEGER NOT NULL DEFAULT 0 CHECK ("concurrency" >= 0), -- 0 = unlimited
    PRIMARY KEY ("queue")
);

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
    "dies_at"           TIMESTAMPTZ NOT NULL,
    "retries"           INTEGER NOT NULL CHECK ("retries" >= 0),
    "initial_retries"   INTEGER NOT NULL CHECK ("initial_retries" >= 0),
    PRIMARY KEY ("id"),
    FOREIGN KEY ("queue") REFERENCES ${"schema"}."queue" ("queue") ON DELETE CASCADE
) PARTITION BY RANGE (id);

UPDATE ${"schema"}."task" t
SET "dies_at" = COALESCE(t."finished_at", t."delayed_at", t."created_at", NOW()) + q."ttl"
FROM ${"schema"}."queue" q
WHERE t."queue" = q."queue"
AND t."dies_at" IS NULL;

ALTER TABLE ${"schema"}."task"
    ALTER COLUMN "dies_at" SET NOT NULL;

-- pgqueue.queue_status_index
-- Covers 'new' and 'retry' states
CREATE INDEX IF NOT EXISTS "task_queue_status_idx"
    ON ${"schema"}."task" ("queue", "delayed_at")
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
        WHEN dies_at < NOW()                                                   THEN 'expired'::${"schema"}.task_status
        WHEN started_at IS NULL AND finished_at IS NULL AND retries = 0        THEN 'failed'::${"schema"}.task_status
        WHEN started_at IS NULL AND finished_at IS NULL AND retries = initial_retries
                                                                            THEN 'new'::${"schema"}.task_status
        WHEN started_at IS NULL AND finished_at IS NULL                        THEN 'retry'::${"schema"}.task_status
        WHEN started_at IS NOT NULL AND finished_at IS NULL                    THEN 'running'::${"schema"}.task_status
        ELSE NULL
    END AS "status";
$$ LANGUAGE SQL STABLE;

-- pgqueue.queue_insert_func
CREATE OR REPLACE FUNCTION ${"schema"}.queue_insert(q TEXT, p JSONB, delayed_at TIMESTAMPTZ) RETURNS BIGINT AS $$
DECLARE
    v_retries       INTEGER;
    v_dies_at       TIMESTAMPTZ;
    v_id            BIGINT;
BEGIN
    -- Get queue defaults, raise if queue doesn't exist
    SELECT
        "retries", CASE
            WHEN delayed_at IS NULL OR delayed_at < NOW() THEN NOW() + "ttl"
            ELSE delayed_at + "ttl"
        END
    INTO STRICT
        v_retries, v_dies_at
    FROM
        ${"schema"}."queue"
    WHERE
        "queue" = q;

    -- Insert the task
    INSERT INTO ${"schema"}."task" ("queue", "payload", "delayed_at", "retries", "initial_retries", "dies_at")
    VALUES (
        q, p, CASE
            WHEN delayed_at IS NULL THEN NULL
            WHEN delayed_at < NOW() THEN NOW()
            ELSE delayed_at
        END, v_retries, v_retries, v_dies_at
    )
    RETURNING "id" INTO v_id;

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
UPDATE ${"schema"}."task" SET
    "started_at" = NOW(),
    "worker" = w,
    "result" = 'null'
WHERE "id" = (
    SELECT
        t."id"
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
        GROUP BY
            "queue"
    ) retained
    ON
        retained."queue" = t."queue"
    WHERE
        (CARDINALITY(q) = 0 OR t."queue" = ANY(q))
    AND
        (t."started_at" IS NULL AND t."finished_at" IS NULL)
    AND
        t."dies_at" > NOW()
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
RETURNING "id";
$$ LANGUAGE SQL;

-- pgqueue.queue_unlock_func
CREATE OR REPLACE FUNCTION ${"schema"}.queue_unlock(tid BIGINT, r JSONB) RETURNS BIGINT AS $$
    UPDATE ${"schema"}."task" SET
        "finished_at" = NOW(),
        "result" = r
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
        "delayed_at" = ${"schema"}.queue_backoff(tid)
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
CREATE OR REPLACE FUNCTION ${"schema"}."queue_clean"(q TEXT) RETURNS TABLE (
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
                    "created_at",
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
            ORDER BY
                "created_at"
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
CREATE OR REPLACE FUNCTION ${"schema"}.ticker_next() RETURNS TABLE (
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
                "interval" IS NOT NULL
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
