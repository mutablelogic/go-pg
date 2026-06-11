/*
Package httphandler provides HTTP handlers for the queue package.

This package implements RESTful HTTP handlers for managing queues, tasks, and tickers.
It follows the same pattern as pkg/manager/httphandler, providing a consistent API
interface for queue operations.

# Namespace Endpoints

	GET    /namespace       - List all namespaces

# Queue Endpoints

	GET    /queue           - List all queues
	POST   /queue           - Create/register a new queue
	GET    /queue/{name}    - Get a specific queue
	PATCH  /queue/{name}    - Update a queue
	DELETE /queue/{name}    - Delete a queue

# Task Endpoints

	GET    /task            - List tasks (optional ?queue, ?status, ?offset, ?limit)
	POST   /task            - Create a new task (requires queue in body)
	PUT    /task            - Retain next available task from any queue (requires ?worker)
	PUT    /task/{queue}    - Retain next available task from specific queue (requires ?worker)
	PATCH  /task/{id}       - Release a task with result (mark as complete or failed)

# Ticker Endpoints

	GET    /ticker          - List all tickers
	POST   /ticker          - Create/register a new ticker
	GET    /ticker/{name}   - Get a specific ticker
	GET    /ticker/next     - SSE stream of matured tickers
	PATCH  /ticker/{name}   - Update a ticker
	DELETE /ticker/{name}   - Delete a ticker

# Metrics Endpoint

	GET    /metrics         - Prometheus metrics

# Usage

To register all handlers:

	import (
		"net/http"
		"github.com/mutablelogic/go-pg/pkg/queue"
		"github.com/mutablelogic/go-pg/pkg/queue/httphandler"
	)

	func main() {
		manager, _ := queue.New(ctx, conn, queue.WithNamespace("myapp"))
		router := http.NewServeMux()

		httphandler.RegisterBackendHandlers(router, "/api", manager)

		http.ListenAndServe(":8080", router)
	}

Or register individual handler groups:

	httphandler.RegisterNamespaceHandlers(router, "/api", manager)
	httphandler.RegisterQueueHandlers(router, "/api", manager)
	httphandler.RegisterTaskHandlers(router, "/api", manager)
	httphandler.RegisterTickerHandlers(router, "/api", manager)
	httphandler.RegisterMetricsHandler(router, "/api", manager)
*/
package httphandler
