
-- pgqueue.get
SELECT
    "queue",
    "ttl",
    "retries",
    "retry_delay",
    "concurrency",
    "dedupe"
FROM
    ${"schema"}."queue"
WHERE
    "queue" = @id;

-- pgqueue.insert
-- Returns no queue if it exists
INSERT INTO ${"schema"}."queue" (
    "queue"
) VALUES (
    @queue
)
ON CONFLICT ("queue") DO NOTHING
RETURNING
    "queue",
    "ttl",
    "retries",
    "retry_delay",
    "concurrency",
    "dedupe";

-- pgqueue.patch
UPDATE
    ${"schema"}."queue"
SET
    ${patch}
WHERE
    "queue" = @id
RETURNING
    "queue",
    "ttl",
    "retries",
    "retry_delay",
    "concurrency",
    "dedupe";

-- pgqueue.delete
DELETE FROM
    ${"schema"}."queue"
WHERE
    "queue" = @id
RETURNING
    "queue",
    "ttl",
    "retries",
    "retry_delay",
    "concurrency",
    "dedupe";

-- pgqueue.list
-- The queues, with the number of their tasks with each status
SELECT
    q."queue",
    q."ttl",
    q."retries",
    q."retry_delay",
    q."concurrency",
    q."dedupe",
    s."new",
    s."running",
    s."retry",
    s."expired",
    s."failed",
    s."done"
FROM
    ${"schema"}."queue" q
JOIN
    ${"schema"}."queue_status" s ON s."queue" = q."queue"
ORDER BY
    q."queue"

-- pgqueue.clean
SELECT
    "id",
    "queue",
    "payload",
    "result",
    "worker",
    "created_at",
    "delayed_at",
    "started_at",
    "finished_at",
    "dies_at",
    "retries"
FROM
    ${"schema"}.queue_clean(@id, CAST(@retention AS INTERVAL));

-- pgqueue.task_get
SELECT
    "id",
    "queue",
    "payload",
    "result",
    "worker",
    "created_at",
    "delayed_at",
    "started_at",
    "finished_at",
    "dies_at",
	"retries",
	${"schema"}.queue_task_status("dies_at", "started_at", "finished_at", "retries", "initial_retries") AS "status"
FROM
    ${"schema"}."task"
WHERE
    "id" = @tid;

-- pgqueue.task_insert
SELECT
    ${"schema"}.queue_insert(
        @queue,
        CAST(@payload AS JSONB),
        CAST(@delayed_at AS TIMESTAMPTZ)
    ) AS "id";

-- pgqueue.task_patch
UPDATE
    ${"schema"}."task"
SET
    ${patch}
WHERE
    "id" = @tid
RETURNING
    "id",
    "queue",
    "payload",
    "result",
    "worker",
    "created_at",
    "delayed_at",
    "started_at",
    "finished_at",
    "dies_at",
    "retries";

-- pgqueue.task_list
SELECT
    sq."id",
    sq."queue",
    sq."payload",
    sq."result",
    sq."worker",
    sq."created_at",
    sq."delayed_at",
    sq."started_at",
    sq."finished_at",
    sq."dies_at",
    sq."retries",
    sq."status"
FROM (
    SELECT
        t."id",
        t."queue",
        t."payload",
        t."result",
        t."worker",
        t."created_at",
        t."delayed_at",
        t."started_at",
        t."finished_at",
        t."dies_at",
        t."retries",
		${"schema"}.queue_task_status(t."dies_at", t."started_at", t."finished_at", t."retries", t."initial_retries") AS "status"
    FROM
        ${"schema"}."task" t
    ${taskwhere}
) sq
${where}
ORDER BY
    sq."created_at",
    sq."id"

-- pgqueue.retain
SELECT
    ${"schema"}.queue_lock(@queues, @worker) AS "id";

-- pgqueue.release
SELECT
    ${"schema"}.queue_unlock(@tid, CAST(@result AS JSONB)) AS "id";

-- pgqueue.fail
SELECT
    ${"schema"}.queue_fail(@tid, CAST(@result AS JSONB)) AS "id";

-- pgqueue.ticker_get
SELECT
    "ticker",
    "payload",
    "interval",
    "last_at"
FROM
    ${"schema"}."ticker"
WHERE
    "ticker" = @id;

-- pgqueue.ticker_insert
-- Returns no ticker if it exists
INSERT INTO ${"schema"}."ticker" (
    "ticker"
) VALUES (
    @ticker
)
ON CONFLICT ("ticker") DO NOTHING
RETURNING
    "ticker",
    "payload",
    "interval",
    "last_at";

-- pgqueue.ticker_patch
UPDATE
    ${"schema"}."ticker"
SET
    ${patch}
WHERE
    "ticker" = @id
RETURNING
    "ticker",
    "payload",
    "interval",
    "last_at";

-- pgqueue.ticker_delete
DELETE FROM
    ${"schema"}."ticker"
WHERE
    "ticker" = @id
RETURNING
    "ticker",
    "payload",
    "interval",
    "last_at";

-- pgqueue.ticker_list
SELECT
    "ticker",
    "payload",
    "interval",
    "last_at"
FROM
    ${"schema"}."ticker"
ORDER BY
    "ticker"

-- pgqueue.ticker_next
SELECT
    "ticker",
    "payload",
    "interval",
    "last_at"
FROM
    ${"schema"}.ticker_next(@tickers);

-- pgqueue.get_status
-- A queue, with the number of its tasks with each status
SELECT
    q."queue",
    q."ttl",
    q."retries",
    q."retry_delay",
    q."concurrency",
    q."dedupe",
    s."new",
    s."running",
    s."retry",
    s."expired",
    s."failed",
    s."done"
FROM
    ${"schema"}."queue" q
JOIN
    ${"schema"}."queue_status" s ON s."queue" = q."queue"
WHERE
    q."queue" = @id;

-- pgqueue.stats
-- Every queue, with the number of its tasks with each status
SELECT
    q."queue",
    q."ttl",
    q."retries",
    q."retry_delay",
    q."concurrency",
    q."dedupe",
    s."new",
    s."running",
    s."retry",
    s."expired",
    s."failed",
    s."done"
FROM
    ${"schema"}."queue" q
JOIN
    ${"schema"}."queue_status" s ON s."queue" = q."queue"
ORDER BY
    q."queue";
    
-- pgqueue.partition_create
CREATE TABLE IF NOT EXISTS ${"schema"}.${"partition"} PARTITION OF ${"schema"}."task"
    FOR VALUES FROM (${start}) TO (${end});

-- pgqueue.partition_list
-- Returns all partitions for the task table, ordered oldest first
SELECT
    c.relname AS "name",
    pg_get_expr(c.relpartbound, c.oid) AS "bounds",
    COALESCE(t."count", 0) AS "count"
FROM pg_inherits i
JOIN pg_class c ON i.inhrelid = c.oid
JOIN pg_class p ON i.inhparent = p.oid
JOIN pg_namespace n ON c.relnamespace = n.oid
LEFT JOIN (
    SELECT
        tableoid AS "oid",
        COUNT(*) AS "count"
    FROM
        ${"schema"}."task"
    WHERE
        ("started_at" IS NULL AND "finished_at" IS NULL AND "retries" > 0)
    OR
        ("started_at" IS NOT NULL AND "finished_at" IS NULL)
    GROUP BY
        tableoid
) t ON t."oid" = c.oid
WHERE p.relname = 'task'
AND n.nspname = ${'schema'}
ORDER BY c.relname ASC;

-- pgqueue.partition_seq
-- Returns the current sequence value for the task table
SELECT last_value FROM ${"schema"}."task_id_seq";

-- pgqueue.partition_seq_min
-- Raises the sequence for the task table so the next value is at least the
-- minimum, when it's lower, such as when it's been reset, and returns its value
SELECT
    CASE WHEN last_value < @min
    THEN setval(format('%I.%I', ${'schema'}, 'task_id_seq')::regclass, @min, false)
    ELSE last_value END
FROM ${"schema"}."task_id_seq";

-- pgqueue.partition_drop
DROP TABLE IF EXISTS ${"schema"}.${"partition"};