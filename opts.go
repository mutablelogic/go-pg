package pg

import (
	"fmt"
	"net"
	"net/url"
	"slices"
	"sort"
	"strings"

	trace "go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type opt struct {
	*tracer
	Verbose bool
	url.Values
	bind *Bind
}

// Opt is a function which applies options for a connection pool
type Opt func(*opt) error

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	DefaultPort     = "5432"
	defaultHost     = "localhost"
	defaultDatabase = "postgres"
	defaultMaxConns = "10"
)

var (
	defaultScheme = []string{"postgres", "postgresql"}
)

////////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// Apply options to the opt struct
func apply(opts ...Opt) (*opt, error) {
	var o opt

	// Set defaults
	o.Values = make(url.Values)
	o.Set("host", "localhost")
	o.Set("port", DefaultPort)
	o.Set("pool_max_conns", defaultMaxConns)
	o.bind = NewBind()

	// Apply options
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}

	// Return success
	return &o, nil
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// WithURL sets connection parameters from a PostgreSQL URL.
func WithURL(value string) Opt {
	return func(o *opt) error {
		url, err := parseUrl(value)
		if err != nil {
			return err
		}
		o.Values.Set("host", url.Hostname())
		o.Values.Set("port", url.Port())
		o.Values.Set("dbname", strings.TrimPrefix(url.Path, "/"))
		if user := url.User.Username(); user != "" {
			o.Values.Set("user", user)
		}
		if password, ok := url.User.Password(); ok {
			o.Values.Set("password", password)
		}
		q := url.Query()
		for key, values := range q {
			for _, v := range values {
				o.Values.Add(key, v)
			}
		}
		return nil
	}
}

// WithCredentials sets the connection pool username and password. If the database
// name is not set, then the username will be used as the default database name.
func WithCredentials(user, password string) Opt {
	return func(o *opt) error {
		if user != "" {
			o.Set("user", user)
		}
		if password != "" {
			o.Set("password", password)
		}
		// TODO: Possible the db name is not being correctly set
		if !o.Has("dbname") && user != "" {
			o.Set("dbname", user)
		}

		// Return success
		return nil
	}
}

// WithDatabase sets the database name for the connection. If the user name is not set,
// then the database name will be used as the user name.
func WithDatabase(name string) Opt {
	return func(o *opt) error {
		if name == "" {
			o.Del("dbname")
		} else {
			o.Set("dbname", name)
		}
		if !o.Has("user") && name != "" {
			o.Set("user", name)
		}
		return nil
	}
}

// WithSchemaSearchPath sets the schema search path for the connection.
// If no schemas are provided, the search_path parameter is removed.
func WithSchemaSearchPath(schemas ...string) Opt {
	return func(o *opt) error {
		if len(schemas) > 0 {
			o.Set("search_path", strings.Join(schemas, ","))
		} else {
			o.Del("search_path")
		}
		return nil
	}
}

// WithAddr sets the address (host) or (host:port) for the connection.
func WithAddr(addr string) Opt {
	return func(o *opt) error {
		if !strings.Contains(addr, ":") {
			return WithHostPort(addr, DefaultPort)(o)
		} else if host, port, err := net.SplitHostPort(addr); err != nil {
			return err
		} else {
			return WithHostPort(host, port)(o)
		}
	}
}

// WithHostPort sets the hostname and port for the connection. If the port is not set,
// then the default port 5432 will be used.
func WithHostPort(host, port string) Opt {
	return func(o *opt) error {
		if host != "" {
			o.Set("host", host)
		}
		if port != "" {
			o.Set("port", port)
		}
		return nil
	}
}

// WithSSLMode sets the PostgreSQL SSL mode. Valid values are "disable", "allow",
// "prefer", "require", "verify-ca", "verify-full".
func WithSSLMode(mode string) Opt {
	return func(o *opt) error {
		if mode != "" {
			o.Set("sslmode", mode)
		}
		return nil
	}
}

// WithApplicationName sets the application name for the connection.
// This appears in pg_stat_activity and helps identify connections.
func WithApplicationName(name string) Opt {
	return func(o *opt) error {
		if name != "" {
			o.Set("application_name", name)
		}
		return nil
	}
}

// WithTrace sets the trace function for the connection pool.
func WithTrace(fn TraceFn) Opt {
	return func(o *opt) error {
		o.tracer = NewTracer(fn)
		return nil
	}
}

// WithTracer sets the OTEL tracer for the connection pool.
func WithTracer(tracer trace.Tracer) Opt {
	return func(o *opt) error {
		o.tracer = NewOTELTracer(tracer)
		return nil
	}
}

// WithBind sets a bind variable for the connection pool.
func WithBind(k string, v any) Opt {
	return func(o *opt) error {
		o.bind.Set(k, v)
		return nil
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (o *opt) encode(skip ...string) []string {
	// We sort the keys to ensure that the URL is deterministic
	keys := make([]string, 0, len(o.Values))
	for key := range o.Values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Encode the values
	var parts []string
	for _, key := range keys {
		if slices.Contains(skip, key) {
			continue
		}
		if value := o.Values.Get(key); value != "" {
			parts = append(parts, fmt.Sprintf("%v=%v", key, o.Values.Get(key)))
		}
	}

	return parts
}

// Encode the options as a connection string
func (o *opt) Encode() string {
	return strings.Join(o.encode(), " ")
}

// Parse the URL
func parseUrl(value string) (*url.URL, error) {
	url, err := url.Parse(value)
	if err != nil {
		return nil, err
	}

	// Check scheme
	if url.Scheme == "" {
		url.Scheme = defaultScheme[0]
	} else if !slices.Contains(defaultScheme, url.Scheme) {
		return nil, ErrBadParameter.With("invalid database scheme")
	}

	// Normalize host:port
	if url.Port() == "" {
		url.Host = net.JoinHostPort(url.Host, DefaultPort)
	}
	host, port, err := net.SplitHostPort(url.Host)
	if err != nil {
		return nil, ErrBadParameter.With("invalid database host format")
	}
	if port == "" {
		port = DefaultPort
	}
	if host == "" {
		host = defaultHost
	}
	url.Host = fmt.Sprintf("%s:%s", host, port)

	// Get user credentials - and set database name if missing
	if url.User != nil {
		if user := url.User.Username(); user != "" && url.Path == "" {
			url.Path = "/" + user
		}
	}

	// Normalize path
	if url.Path == "" || url.Path == "/" {
		url.Path = "/" + defaultDatabase
	}

	// For now, just return the string
	return url, nil
}
