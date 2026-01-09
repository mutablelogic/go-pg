// Package httpclient provides a typed Go client for consuming the PostgreSQL
// queue management REST API.
//
// Create a client with:
//
// client, err := httpclient.New("http://localhost:8080/api/v1")
//
//	if err != nil {
//	   panic(err)
//	}
//
// Then use the client to query resources:
//
// queues, err := client.ListQueues(ctx)
// tasks, err := client.ListTasks(ctx)
// tickers, err := client.ListTickers(ctx)
package httpclient
