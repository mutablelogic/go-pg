
-- pgqueue.insert
INSERT INTO queue (
    ns, queue, ttl, retries, retry_delay
) VALUES (
    @ns, @queue, DEFAULT, DEFAULT, DEFAULT
) RETURNING 
    queue, ttl, retries, retry_delay, ns

-- pgqueue.get
SELECT
    queue, ttl, retries, retry_delay, ns
FROM
    queue
WHERE
    queue = @id
AND
    ns = @ns

-- pgqueue.patch
UPDATE queue SET
    ${patch}
WHERE
    queue = @id
AND
    ns = @ns
RETURNING
    queue, ttl, retries, retry_delay, ns

-- pgqueue.delete
DELETE FROM queue WHERE
    queue = @id
AND
    ns = @ns
RETURNING
    queue, ttl, retries, retry_delay, ns

-- pgqueue.list
SELECT
    queue, ttl, retries, retry_delay, ns
FROM 
    queue 
WHERE
    ns = @ns ${where}

-- pgqueue.clean
SELECT * FROM queue_clean(@ns, @id)

-- pgqueue.stats
SELECT qs."queue", qs."status", qs."count"
FROM queue_status qs
JOIN queue q ON qs."queue" = q."queue"
WHERE q."ns" = @ns ${where}

-- pgqueue.namespace_list
SELECT DISTINCT "ns" FROM (
    SELECT "ns" FROM queue
    UNION
    SELECT "ns" FROM ticker
) AS namespaces ORDER BY "ns"

-- pgqueue.retain
-- Returns the id of the task which has been retained
-- @queues is a TEXT[] array, empty array means any queue
SELECT queue_lock(@ns, @queues::text[], @worker)

-- pgqueue.release
-- Returns the id of the task which has been released
SELECT queue_unlock(@tid, @result)

-- pgqueue.fail
-- Returns the id of the task which has been failed
SELECT queue_fail(@tid, @result)

-- pgqueue.task_insert
-- Inserts a new task into the queue and returns the task id
SELECT queue_insert(@ns, @queue, @payload::jsonb, @delayed_at)

-- pgqueue.task_select
SELECT 
    "id", "queue", "ns", "payload", "result", "worker", "created_at", "delayed_at", "started_at", "finished_at", "dies_at", "retries", queue_task_status("id") AS "status"
FROM
    "task"

-- pgqueue.task_get
${pgqueue.task_select} WHERE "id" = @tid

-- pgqueue.task_list
${pgqueue.task_select} ${where}

-- pgqueue.ticker_insert
INSERT INTO ticker 
    ("ns", "ticker", "interval", "ts") 
VALUES 
    (@ns, @id, DEFAULT, DEFAULT)
RETURNING
    "ticker", "interval", "ns", "payload", "ts"

-- pgqueue.ticker_patch
UPDATE ticker SET
    ${patch},
    "ts" = NULL
WHERE
    "ns" = @ns AND "ticker" = @id
RETURNING
    "ticker", "interval", "ns", "payload", "ts"


-- pgqueue.ticker_delete
		DELETE FROM 
			ticker
		WHERE 
			"ns" = @ns AND "ticker" = @id
		RETURNING
			"ticker", "interval",  "ns", "payload", "ts"

-- pgqueue.ticker_select
SELECT
    "ticker", "interval", "ns", "payload", "ts"
FROM
    ticker

-- pgqueue.ticker_list
SELECT
    "ticker", "interval", "ns", "payload", "ts"
FROM
    ticker
WHERE "ns" = @ns

-- pgqueue.ticker_get
SELECT
    "ticker", "interval", "ns", "payload", "ts"
FROM
    ticker
WHERE "ns" = @ns AND "ticker" = @id

-- pgqueue.ticker_next
SELECT * FROM ticker_next(@ns)


