# Task Queue

## Queue Operations

Queues hold tasks, which are run by the workers with a callback for the queue. A queue has a `ttl`, which is how long a task can run before another worker can retain it, the number of `retries` a task has, the `retry_delay` before a failed task is retried, which increases with each retry, the `concurrency`, which is how many of its tasks can run at once, or zero for no limit, and whether it deduplicates tasks with `dedupe`, so a task isn't inserted while the same task is pending.

### GET /queue

List queues in name order, a page at a time. A page has at most `limit` queues, which defaults to and is capped at 100, starting at `offset`, and `count` is the total number of queues. Returns 400 if the query is invalid.

## Ticker Operations

Tickers run a callback periodically, at their `interval`, with their `payload`, on one of the workers with a callback for the ticker.

### GET /ticker

List tickers in name order, a page at a time. A page has at most `limit` tickers, which defaults to and is capped at 100, starting at `offset`, and `count` is the total number of tickers. Each ticker has its `payload`, its `interval`, and `last_at`, when it last ran. Returns 400 if the query is invalid.

## Task Operations

Tasks are run by the workers with a callback for their queue, at most once at a time, and are retried when they fail, until they have no retries left. A task's `payload` is passed to the callback, and its `result` is the callback's result, or its error when it fails. Finished tasks are kept for a retention period, and then deleted.

### GET /task

List tasks, oldest first, a page at a time. The `queue` parameter, which can be given more than once, returns only the tasks in those queues, and the `status` parameter returns only the tasks with that status, which is `new`, `running`, `retry`, `expired`, `failed` or `done`, or any status without it. A page has at most `limit` tasks, which defaults to and is capped at 100, starting at `offset`, and `count` is the total number of tasks matching. Each task has its `queue`, `payload`, `result`, `status`, the `worker` which retained it, and when it was created, delayed until, started, and finished, and when it expires. Returns 400 if the query is invalid, such as an invalid queue name, or an unknown status.
