
-- pgqueue.queue
CREATE TABLE IF NOT EXISTS queue (
    "ns" TEXT NOT NULL,                                               -- namespace and queue name
    "queue" TEXT NOT NULL,
    "ttl" INTERVAL DEFAULT INTERVAL '1 hour',                        -- time-to-live for queue messages
    "retries" INTEGER NOT NULL DEFAULT 3 CHECK ("retries" >= 0),     -- number of retries before failing
    "retry_delay" INTERVAL NOT NULL DEFAULT INTERVAL '2 minute',     -- delay between retries in seconds
    PRIMARY KEY ("ns", "queue")                                      -- primary key
);

-- pgqueue.queue_task_status_type
-- Create the status type
DO $$ BEGIN
    CREATE TYPE STATUS AS ENUM('expired', 'new', 'failed', 'retry', 'retained', 'released', 'unknown');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- pgqueue.task
CREATE TABLE IF NOT EXISTS "task" (
    "id"                         BIGSERIAL PRIMARY KEY,     -- task id
    "ns"                         TEXT NOT NULL,     -- namespace and queue
    "queue"                      TEXT NOT NULL,
    "payload"                    JSONB NOT NULL DEFAULT '{}',     -- task payload and result
    "result"                     JSONB NOT NULL DEFAULT 'null',     -- result is used for both successful and unsuccessful tasks
    "worker"                     TEXT,     -- worker identifier
    "created_at"                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),     -- when the task has been created
    "delayed_at"                 TIMESTAMPTZ,     -- when the task should be next run (or NULL if it should be run now)
    "started_at"                 TIMESTAMPTZ,     -- when the task has been started, finished and when it expires (dies)
    "finished_at"                TIMESTAMPTZ,
    "dies_at"                    TIMESTAMPTZ,
    "retries"                    INTEGER NOT NULL CHECK ("retries" >= 0),     -- task maximum retries. when this reaches zero the task is marked as failed
    "initial_retries"            INTEGER NOT NULL CHECK ("initial_retries" >= 0),     -- the initial retry value
    FOREIGN KEY ("ns", "queue") REFERENCES queue ("ns", "queue") ON DELETE CASCADE     -- foreign key (ns, queue) references queue (ns, queue)
);

-- pgqueue.task_indexes
-- Index for efficient task locking by namespace and queue
CREATE INDEX IF NOT EXISTS idx_task_lock ON "task" ("ns", "queue", "created_at") 
WHERE "started_at" IS NULL AND "finished_at" IS NULL AND "retries" > 0;

-- Index for namespace and queue filtering
CREATE INDEX IF NOT EXISTS idx_task_ns_queue ON "task" ("ns", "queue");

-- pgqueue.ticker
CREATE TABLE IF NOT EXISTS ticker (
    -- ticker namespace and name
    "ns"                   TEXT NOT NULL,
    "ticker"               TEXT NOT NULL,
    "payload"              JSONB NOT NULL DEFAULT '{}',
    -- interval (NULL means disabled)
    "interval"             INTERVAL DEFAULT INTERVAL '1 minute',
    -- last tick
    "ts"                   TIMESTAMPTZ,
    -- primary key
    PRIMARY KEY ("ns", "ticker")
);

-- pgqueue.ticker_indexes
-- Index for efficient ticker selection by namespace and timestamp
CREATE INDEX IF NOT EXISTS idx_ticker_next ON ticker ("ns", "ts" NULLS FIRST) 
WHERE "interval" IS NOT NULL;

-- pgqueue.queue_task_status_func
-- Return the current status of a task
CREATE OR REPLACE FUNCTION queue_task_status(t BIGINT) RETURNS STATUS AS $$
    SELECT CASE
        WHEN "dies_at" IS NOT NULL AND "dies_at" < NOW() THEN 'expired'::STATUS
        WHEN "started_at" IS NULL AND "finished_at" IS NULL  AND "retries" = "initial_retries" THEN 'new'::STATUS
        WHEN "started_at" IS NULL AND "finished_at" IS NULL  AND "retries" = 0 THEN 'failed'::STATUS
        WHEN "started_at" IS NULL AND "finished_at" IS NULL  THEN 'retry'::STATUS
        WHEN "started_at" IS NOT NULL AND "finished_at" IS NULL  THEN 'retained'::STATUS
        WHEN "started_at" IS NOT NULL AND "finished_at" IS NOT NULL THEN 'released'::STATUS
        ELSE 'unknown'::STATUS
    END AS "status"
    FROM 
        "task"
    WHERE
        "id" = t;              
$$ LANGUAGE SQL;

-- pgqueue.queue_insert_func
-- Insert a new payload into a queue
CREATE OR REPLACE FUNCTION queue_insert(n TEXT, q TEXT, p JSONB, delayed_at TIMESTAMPTZ) RETURNS BIGINT AS $$
WITH defaults AS (
    -- Select the retries and ttl from the queue defaults
    SELECT
        retries, CASE 
            WHEN "ttl" IS NULL THEN NULL 
            WHEN "delayed_at" IS NULL OR "delayed_at" < NOW() THEN NOW() + "ttl"                    
            ELSE "delayed_at" + "ttl" 
        END AS dies_at
    FROM
        "queue"
    WHERE
        "queue" = q AND "ns" = n
    LIMIT
        1
) INSERT INTO 
    "task" ("ns", "queue", "payload", "delayed_at", "retries", "initial_retries", "dies_at")
SELECT
    n, q, p, CASE
        WHEN "delayed_at" IS NULL THEN NULL
        WHEN "delayed_at" < NOW() THEN NOW()
        ELSE "delayed_at"
    END, retries, retries, dies_at
FROM
    defaults
RETURNING
    "id"
$$ LANGUAGE SQL;

-- pgqueue.queue_notify_func
CREATE OR REPLACE FUNCTION queue_notify() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(NEW.ns || '_queue_insert', NEW.queue);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- pgqueue.queue_insert_trigger
CREATE OR REPLACE TRIGGER 
    queue_insert_trigger 
AFTER INSERT ON "task" FOR EACH ROW EXECUTE FUNCTION 
    queue_notify();

-- pgqueue.queue_lock
-- A specific worker locks a task in a queue for processing
CREATE OR REPLACE FUNCTION queue_lock(n TEXT, q TEXT, w TEXT) RETURNS BIGINT AS $$
UPDATE "task" SET 
    "started_at" = NOW(), "worker" = w, "result" = 'null'
WHERE "id" = (
    SELECT 
        "id" 
    FROM
        "task"
    WHERE
        "ns" = n
    AND
        "queue" = q
    AND
        ("started_at" IS NULL AND "finished_at" IS NULL AND "dies_at" > NOW())
    AND 
        ("delayed_at" IS NULL OR "delayed_at" <= NOW())
    AND
        ("retries" > 0)
    ORDER BY
        "created_at"
    FOR UPDATE SKIP LOCKED LIMIT 1
) RETURNING
    "id";
$$ LANGUAGE SQL;

-- pgqueue.queue_unlock_func
-- Unlock a task in a queue with successful result
CREATE OR REPLACE FUNCTION queue_unlock(tid BIGINT, r JSONB) RETURNS BIGINT AS $$
    UPDATE "task" SET 
        "finished_at" = NOW(), "dies_at" = NULL, "result" = r
    WHERE 
        ("id" = tid)
    AND
        ("started_at" IS NOT NULL AND "finished_at" IS NULL)
    RETURNING
        "id";
$$ LANGUAGE SQL;

-- pgqueue.queue_backoff_func
-- Calculate the backoff time for a task based on its retries
CREATE OR REPLACE FUNCTION queue_backoff(tid BIGINT) RETURNS TIMESTAMPTZ AS $$
    SELECT CASE
        WHEN T."retries" = 0 THEN NULL
        ELSE NOW() + Q."retry_delay" * (POW(T."initial_retries" - T."retries" + 1,2))
    END AS "delayed_at"
    FROM
        "task" T
    JOIN            	
        "queue" Q
    ON
        T."queue" = Q."queue" AND T."ns" = Q."ns"
    WHERE
        T."id" = tid;
$$ LANGUAGE SQL;

-- pgqueue.queue_fail_func
-- Unlock a task in a queue with fail result
CREATE OR REPLACE FUNCTION queue_fail(tid BIGINT, r JSONB) RETURNS BIGINT AS $$
    UPDATE "task" SET 
        "retries" = "retries" - 1, "result" = r, "started_at" = NULL, "finished_at" = NULL, "delayed_at" = queue_backoff(tid)
    WHERE 
        "id" = tid AND "retries" > 0 AND ("started_at" IS NOT NULL AND "finished_at" IS NULL)
    RETURNING
        "id";
$$ LANGUAGE SQL;

-- pgqueue.queue_clean_func
-- Cleanup tasks in a queue which are in an end state 
CREATE OR REPLACE FUNCTION queue_clean(n TEXT, q TEXT) RETURNS TABLE (
    "id" BIGINT, "queue" TEXT, "ns" TEXT, "payload" JSONB, "result" JSONB, "worker" TEXT, "created_at" TIMESTAMPTZ, "delayed_at" TIMESTAMPTZ, "started_at" TIMESTAMPTZ, "finished_at" TIMESTAMPTZ, "dies_at" TIMESTAMPTZ, "retries" INTEGER
) AS $$
    DELETE FROM
        "task"
    WHERE
        id IN (
            WITH sq AS (
                SELECT 
                    "id", queue_task_status("id") AS "status" 
                FROM 
                    "task" 
                WHERE
                    "ns" = n AND "queue" = q
            ) SELECT 
                "id" 
            FROM 
                sq 
            WHERE 
                "status" IN ('expired', 'released', 'failed')
            ORDER BY 
                "created_at"
            LIMIT 
                100
        )
    RETURNING
        "id", "queue", "ns", "payload", "result", "worker", "created_at", "delayed_at", "started_at", "finished_at", "dies_at", "retries";
$$ LANGUAGE SQL;

-- pgqueue.ticker_next_func
-- Return the next matured ticker for a namespace
CREATE OR REPLACE FUNCTION ticker_next(namespace TEXT) RETURNS TABLE (
    "ticker" TEXT, "interval" INTERVAL, "ns" TEXT, "payload" JSONB, "ts" TIMESTAMPTZ
) AS $$
    UPDATE
        ticker
    SET
        "ts" = NOW()
    WHERE
        "ns" = namespace AND "ticker" = (
            SELECT "ticker" 
            FROM ticker 
            WHERE "ns" = namespace 
            AND ("ts" IS NULL OR "ts" + "interval" <= NOW())
            ORDER BY "ts" NULLS FIRST
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
    RETURNING		
        "ticker", "interval", "ns", "payload", "ts";
$$ LANGUAGE SQL;

-- pgqueue.queue_status_view
-- View showing the status of each queue
CREATE OR REPLACE VIEW "queue_status" AS
SELECT 
    Q."ns", Q."queue", S."status", COUNT(T."id") AS "count"
FROM
    "queue" Q
CROSS JOIN
    (SELECT UNNEST(enum_range(NULL::STATUS)) AS "status") S
LEFT JOIN
    (SELECT "id", "ns", "queue", queue_task_status(id) AS "status" FROM "task") T
ON
    S."status" = T."status" AND Q."queue" = T."queue" AND Q."ns" = T."ns"
GROUP BY
    1, 2, 3;
