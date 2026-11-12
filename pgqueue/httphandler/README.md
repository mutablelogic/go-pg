# Task Queue

## Queue Operations

Queues hold tasks, which are run by the workers with a callback for the queue. A queue has a `ttl`, which is how long a task can run before another worker can retain it, the number of `retries` a task has, the `retry_delay` before a failed task is retried, which increases with each retry, the `concurrency`, which is how many of its tasks can run at once, or zero for no limit, and whether it deduplicates tasks with `dedupe`, so a task isn't inserted while the same task is pending.

### GET /queue

List queues in name order, a page at a time, with the number of each queue's tasks with each status: `new`, `delayed`, `running`, `retry`, `failed` and `done`. A page has at most `limit` queues, which defaults to and is capped at 100, starting at `offset`, and `count` is the total number of queues. Returns 400 if the query is invalid.

### GET /queue/{name}

Get a queue by name, with the number of its tasks with each status: `new`, `delayed`, `running`, `retry`, `failed` and `done`. Returns 400 if the name is invalid, or 404 if there's no queue with the name.

### PATCH /queue/{name}

Change a queue's settings, and return the queue.

- The body has any of `ttl`, `retries`, `retry_delay`, `concurrency` and `dedupe`. Durations are in nanoseconds.
- Settings which aren't in the body are kept.
- A process which registers the queue with settings sets them again when it starts.
- Returns 400 if the name or a setting is invalid, or there are none, or 404 if there's no queue with the name.

### DELETE /queue/{name}

Delete a queue, with all its tasks, including those which are running, and return it.

- A process which registers the queue creates it again when it starts.
- Returns 400 if the name is invalid, or 404 if there's no queue with the name.

### POST /queue/{name}/clean

Delete up to 100 of a queue's `done` and `failed` tasks, which finished longer ago than the retention period, oldest first, and return them in `body`. Workers clean the queues periodically, so this is only needed to clean them sooner. Returns 400 if the name is invalid.

## Ticker Operations

Tickers run a callback periodically, at their `interval`, with their `payload`, on one of the workers with a callback for the ticker.

### GET /ticker

List tickers in name order, a page at a time. A page has at most `limit` tickers, which defaults to and is capped at 100, starting at `offset`, and `count` is the total number of tickers. Each ticker has its `payload`, its `interval`, and `last_at`, when it last ran. Returns 400 if the query is invalid.

### GET /ticker/{name}

Get a ticker by name, with its `payload`, its `interval`, and `last_at`, when it last ran. Returns 400 if the name is invalid, or 404 if there's no ticker with the name.

## Task Operations

Tasks are run by the workers with a callback for their queue, at most once at a time, and are retried when they fail, until they have no retries left. A task's `payload` is passed to the callback, and its `result` is the callback's result, or its error when it fails. Finished tasks are kept for a retention period, and then deleted.

### GET /task

List tasks, a page at a time, by priority, highest first, where 0 is the highest, and then oldest first. The `queue` parameter, which can be given more than once, returns only the tasks in those queues, and the `status` parameter, which can also be given more than once, returns only the tasks with those statuses, which are `new`, `delayed`, `running`, `retry`, `failed` or `done`, or with any status without it. A page has at most `limit` tasks, which defaults to and is capped at 100, starting at `offset`, and `count` is the total number of tasks matching. Each task has its `queue`, `payload`, `result`, `status`, `priority`, the `worker` which retained it, and when it was created, delayed until, started, and finished, and when it expires. Returns 400 if the query is invalid, such as an invalid queue name, or an unknown status.

### GET /task/{id}

Get a task by id, with its status, to examine it. The task isn't retained or changed. Returns 400 if the id isn't a number, or 404 if there's no task with the id, such as when it's been deleted after the retention period.

### PATCH /task/{id}

Change a task's `priority`, and return the task, with its status.

- The body has the `priority`, from 0, the highest, to 32767. Lower runs first.
- Only a task which hasn't run yet can be changed: one which is `new`, `delayed`, or waiting to be retried (`retry`).
- Returns 400 if the id or priority is invalid, 404 if there's no task with the id, or 409 if the task is running or has finished.
