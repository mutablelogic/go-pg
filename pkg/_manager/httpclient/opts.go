package httpclient

import (
	"fmt"
	"net/url"

	// Packages
	types "github.com/mutablelogic/go-server/pkg/types"
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

func WithForce(v bool) Opt {
	if v {
		return OptSet("force", fmt.Sprint(v))
	} else {
		return OptSet("force", "")
	}
}

func WithDatabase(v *string) Opt {
	return OptSet("database", types.PtrString(v))
}

func WithRole(v *string) Opt {
	return OptSet("role", types.PtrString(v))
}

func WithState(v *string) Opt {
	return OptSet("state", types.PtrString(v))
}

func WithSchema(v *string) Opt {
	return OptSet("schema", types.PtrString(v))
}

func WithCategory(v *string) Opt {
	return OptSet("category", types.PtrString(v))
}

func WithReload(v bool) Opt {
	if v {
		return OptSet("reload", "true")
	}
	return OptSet("reload", "")
}

func WithType(v *string) Opt {
	return OptSet("type", types.PtrString(v))
}

func WithInstalled(v *bool) Opt {
	return func(o *opt) error {
		if v == nil {
			o.Del("installed")
		} else if *v {
			o.Set("installed", "true")
		} else {
			o.Set("installed", "false")
		}
		return nil
	}
}

func OptDatabase(v string) Opt {
	return OptSet("database", v)
}

func OptRole(v string) Opt {
	return OptSet("role", v)
}

func OptState(v string) Opt {
	return OptSet("state", v)
}

func OptCascade(v bool) Opt {
	if v {
		return OptSet("cascade", "true")
	}
	return OptSet("cascade", "")
}

func WithOrderBy(v string) Opt {
	return OptSet("order_by", v)
}

func WithOrderDir(v string) Opt {
	return OptSet("order_dir", v)
}

func WithSort(v string) Opt {
	return OptSet("sort", v)
}

func OptSet(k, v string) Opt {
	return func(o *opt) error {
		if v == "" {
			o.Del(k)
		} else {
			o.Set(k, v)
		}
		return nil
	}
}
