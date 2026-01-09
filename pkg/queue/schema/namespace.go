package schema

import (
	// Packages
	pg "github.com/mutablelogic/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type NamespaceListRequest struct{}

type NamespaceList struct {
	Count uint64   `json:"count"`
	Body  []string `json:"body,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (l NamespaceList) String() string {
	return stringify(l)
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (l *NamespaceList) Scan(row pg.Row) error {
	var namespace string
	if err := row.Scan(&namespace); err != nil {
		return err
	}
	l.Body = append(l.Body, namespace)
	return nil
}

func (l *NamespaceList) ScanCount(row pg.Row) error {
	return row.Scan(&l.Count)
}

////////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (l NamespaceListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	switch op {
	case pg.List:
		return bind.Replace("${pgqueue.namespace_list}"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("Unsupported NamespaceListRequest operation %q", op)
	}
}
