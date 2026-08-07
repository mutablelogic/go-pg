package schema

import (
	"strings"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type NodeName string

type NodeMeta struct {
	Description *string `json:"description,omitempty" help:"Human-readable description"`
}

type NodeInsert struct {
	Name string `json:"name" arg:"" help:"Node name"`
	In   Edge   `json:"in,omitempty" help:"Incoming edge to this node"`
	Out  []Edge `json:"out,omitempty" help:"Outgoing edges from this node"`
	NodeMeta
}

type Node struct {
	NodeInsert
	Version     string `json:"version" help:"Node version"`
	RetainCount uint64 `json:"retain_count" help:"Retained node value"`
}

type Edge string

////////////////////////////////////////////////////////////////////////////////
// READER

func (n *Node) Scan(row pg.Row) error {
	return row.Scan(&n.Name, &n.Version, &n.Description, &n.RetainCount)
}

////////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (n NodeName) Select(bind *pg.Bind, op pg.Op) (string, error) {
	if name := strings.TrimSpace(string(n)); !types.IsIdentifier(name) {
		return "", pg.ErrBadParameter.With("node name is not a valid identifier")
	} else {
		bind.Set("name", name)
	}
	if !bind.Has("version") {
		return "", pg.ErrBadParameter.With("version is missing")
	}

	switch op {
	case pg.Get:
		return bind.Query("pggraph.node_get"), nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported NodeName operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

func (n NodeInsert) Insert(bind *pg.Bind) (string, error) {
	if name := strings.TrimSpace(n.Name); !types.IsIdentifier(name) {
		return "", pg.ErrBadParameter.With("name is missing or not a valid identifier")
	} else {
		bind.Set("name", name)
	}
	if in := strings.TrimSpace(string(n.In)); in == "" {
		return "", pg.ErrBadParameter.Withf("node %q requires one incoming edge", n.Name)
	} else {
		bind.Set("in", in)
	}
	if len(n.Out) == 0 {
		return "", pg.ErrBadParameter.Withf("node %q requires one or more outgoing edges", n.Name)
	}
	out := make([]string, 0, len(n.Out))
	for _, edge := range n.Out {
		if edgeType := strings.TrimSpace(string(edge)); edgeType == "" {
			return "", pg.ErrBadParameter.Withf("node %q requires one or more outgoing edges", n.Name)
		} else {
			out = append(out, edgeType)
		}
	}
	bind.Set("out", out)
	bind.Set("description", n.Description)
	return bind.Query("pggraph.node_upsert"), nil
}

func (n NodeMeta) Update(bind *pg.Bind) error {
	return nil
}
