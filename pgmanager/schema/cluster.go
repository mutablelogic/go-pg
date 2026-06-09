package schema

import (
	pg "github.com/mutablelogic/go-pg"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type Cluster struct {
	Name string
}

////////////////////////////////////////////////////////////////////////////////
// SELECT

func (d Cluster) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Return query
	switch op {
	case pg.Get:
		return sqlClusterName, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported Cluster operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (c *Cluster) Scan(row pg.Row) error {
	return row.Scan(&c.Name)
}

////////////////////////////////////////////////////////////////////////////////
// SQL

const (
	sqlClusterName = `
		SELECT COALESCE(
				NULLIF(current_setting('cluster_name', true), ''),
				CONCAT_WS(
					':',
					host(inet_server_addr()),
					inet_server_port()::text
				)
		) AS cluster_name
	`
)
