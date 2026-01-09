package httpclient
package httpclient

import (
	"fmt"
	"net/url"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type opt struct {
	url.Values
}

// Opt is an option to set on the client request.
type Opt func(*opt) error

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func applyOpts(opts ...Opt) (*opt, error) {
	o := new(opt)
	o.Values = make(url.Values)
	for _, opt := range opts {
		if err := opt(o); err != nil {
			return nil, err
		}
	}
	return o, nil
}

////////////////////////////////////////////////////////////////////////////////
// OPTIONS

// WithOffsetLimit sets offset and limit query parameters.
func WithOffsetLimit(offset uint64, limit *uint64) Opt {
	return func(o *opt) error {
		if offset > 0 {
			o.Set("offset", fmt.Sprint(offset))
		}
		if limit != nil {
			o.Set("limit", fmt.Sprint(*limit))
		}
		return nil
	}
}

// WithQueue sets the queue query parameter.
func WithQueue(queue string) Opt {
	return func(o *opt) error {
		if queue != "" {
			o.Set("queue", queue)
		}
		return nil
	}
}

// WithWorker sets the worker query parameter.
func WithWorker(worker string) Opt {
	return func(o *opt) error {
		if worker != "" {
			o.Set("worker", worker)
		}
		return nil
	}
}

// OptSet is a generic option to set a query parameter.
func OptSet(key, value string) Opt {
	return func(o *opt) error {
		if value != "" {
			o.Set(key, value)
		}
		return nil
	}
}
