package schema

import (
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Postmaster is when the server started, which is later once it's
// restarted
type Postmaster struct {
	Started time.Time
}

// Restart is the server, once it's restarted: when it started
type Restart struct {
	Started time.Time `json:"started"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (r Restart) String() string {
	return types.Stringify(r)
}

////////////////////////////////////////////////////////////////////////////////
// SELECT

func (Postmaster) Select(bind *pg.Bind, op pg.Op) (string, error) {
	switch op {
	case pg.Get:
		return `SELECT pg_postmaster_start_time()`, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported Postmaster operation %q", op)
	}
}

func (p *Postmaster) Scan(row pg.Row) error {
	return row.Scan(&p.Started)
}
