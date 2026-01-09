/*
Package httphandler provides HTTP handlers for the queue package.

This package implements RESTful HTTP handlers for managing queues, tasks, and tickers.
It follows the same pattern as pkg/manager/httphandler, providing a consistent API
interface for queue operations.

# Queue Endpoints

	GET    /queue           - List all queues
	POST   /queue           - Create/register a new queue
	GET    /queue/{name}    - Get a specific queue
	PATCH  /queue/{name}    - Update a queue
	DELETE /queue/{name}    - Delete a queue

# Task Endpoints

	GET    /task            - List tasks (optional ?queue, ?status, ?offset, ?limit)
	POST   /task            - Create a new task (requires queue in body)
	GET    /task/{queue}    - Retain next available task (requires ?worker parameter)
	PATCH  /task/{id}       - Release a task with result (mark as complete or failed)

# Ticker Endpoints

	GET    /ticker          - List all tickers
	POST   /ticker          - Create/register a new ticker
	GET    /ticker/{name}   - Get a specific ticker
	PATCH  /ticker/{name}   - Update a ticker
	DELETE /ticker/{name}   - Delete a ticker

# Usage

To register all handlers:

	import (
		"net/http"
		"github.com/mutablelogic/go-pg/pkg/queue"
		"github.com/mutablelogic/go-pg/pkg/queue/httphandler"
	)

	func main() {
		manager := queue.NewManager(conn, "myapp")
		router := http.NewServeMux()

		httphandler.RegisterBackendHandlers(router, "/api", manager)

		http.ListenAndServe(":8080", router)
	}

Or register individual handler groups:

	httphandler.RegisterQueueHandlers(router, "/api", manager)
	httphandler.RegisterTaskHandlers(router, "/api", manager)
	httphandler.RegisterTickerHandlers(router, "/api", manager)
*/
package httphandler
