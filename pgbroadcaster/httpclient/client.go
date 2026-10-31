package httpclient

import (
	// Packages
	client "github.com/mutablelogic/go-client"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// Client is an events HTTP client that wraps the base HTTP client and
// provides typed methods for interacting with the events API.
type Client struct {
	*client.Client
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// New creates a new events HTTP client with the given base URL and options.
// The url parameter should point to the API endpoint, e.g.
// "http://localhost:8084/api".
func New(url string, opts ...client.ClientOpt) (*Client, error) {
	c := new(Client)
	cl, err := client.New(append(opts, client.OptEndpoint(url))...)
	if err != nil {
		return nil, err
	}
	c.Client = cl
	return c, nil
}
