package httpclient

import (
	// Packages
	client "github.com/mutablelogic/go-client"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Client is a pgqueue HTTP client that wraps the base HTTP client and
// provides typed methods for interacting with the pgqueue API.
type Client struct {
	*client.Client
	endpoint string
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a new pgqueue HTTP client with the given base URL and options.
// The url parameter should point to the API endpoint, e.g.
// "http://localhost:8080/api" - each method includes its own path, such as
// "queue", matching how httphandler registers its routes.
func New(url string, opts ...client.ClientOpt) (*Client, error) {
	c := &Client{endpoint: url}
	cl, err := client.New(append(opts, client.OptEndpoint(url))...)
	if err != nil {
		return nil, err
	}
	c.Client = cl
	return c, nil
}
