package schema

import (
	"strings"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type GraphName string

type GraphMeta struct {
	Description *string `json:"description,omitempty" help:"Human-readable description of the graph"`
}

type Graph struct {
	Name      string    `json:"graph" arg:"" help:"Graph name"`
	Version   string    `json:"version" help:"Graph version"`
	CreatedAt time.Time `json:"created_at" help:"When the graph was created"`
	GraphMeta
}

type GraphListRequest struct {
	Version *string `json:"version,omitempty" help:"Graph version to filter by"`
	pg.OffsetLimit
}

type GraphList struct {
	GraphListRequest
	Count uint64   `json:"count"`
	Body  []*Graph `json:"body,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (g *Graph) Scan(row pg.Row) error {
	return row.Scan(&g.Name, &g.Version, &g.Description, &g.CreatedAt)
}

////////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (g GraphName) Select(bind *pg.Bind, op pg.Op) (string, error) {
	if name := strings.TrimSpace(string(g)); !types.IsIdentifier(name) {
		return "", pg.ErrBadParameter.With("graph name is not a valid identifier")
	} else {
		bind.Set("name", name)
	}
	if !bind.Has("version") {
		return "", pg.ErrBadParameter.With("version is missing")
	}

	switch op {
	case pg.Get:
		return bind.Query("pggraph.get"), nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported GraphName operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

func (g GraphMeta) Insert(bind *pg.Bind) (string, error) {
	if !bind.Has("name") {
		return "", pg.ErrBadParameter.With("name is missing")
	}
	if !bind.Has("version") {
		return "", pg.ErrBadParameter.With("version is missing")
	}
	bind.Set("description", g.Description)
	return bind.Query("pggraph.upsert"), nil
}

func (g GraphMeta) Update(bind *pg.Bind) error {
	return pg.ErrNotImplemented.With("GraphMeta does not support updates")
}
