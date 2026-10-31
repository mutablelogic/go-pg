# Events

## Event Operations

Events are the changes to the server's data. Each event has the schema and table which changed, and how it changed, as its action: `INSERT`, `UPDATE` or `DELETE`, and it can say which row, or rows, changed, as its `row`, such as an identifier which the rows share. An event doesn't have what changed, so a client which watches events reads what it needs again when one arrives.

### GET /event

Watch events, as a stream of server-sent events, until the client closes the connection. Each event is named `event`, with the event as JSON as its data, and a `ping` event is sent when there are no events for a while, to keep the connection open. Only the events which match the `schema`, `table` and `action` query parameters are sent, each of which matches any event when it isn't set. Events which happen while a client is reading others can be dropped, if there are too many, and events which happen before it connects aren't sent. Returns 400 if the query is invalid, or 406 if the client doesn't accept `text/event-stream`.
