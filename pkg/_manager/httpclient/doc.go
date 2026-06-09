// Package httpclient provides a typed Go client for consuming the PostgreSQL
// management REST API.
//
// Create a client with:
//
//	client, err := httpclient.New("http://localhost:8080/api/v1")
//	if err != nil {
//	    panic(err)
//	}
//
// Then use the client to query resources:
//
//	roles, err := client.ListRoles(ctx)
//	databases, err := client.ListDatabases(ctx, httpclient.WithDatabase("mydb"))
package httpclient
