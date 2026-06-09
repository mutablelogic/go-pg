package manager

import (
	"context"
	"errors"
	"fmt"

	"github.com/mutablelogic/go-pg"
	"github.com/mutablelogic/go-pg/pgmanager/schema"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type Manager struct {
	opts
	conn pg.PoolConn
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func New(conn pg.PoolConn, opt ...Opt) (*Manager, error) {
	self := new(Manager)

	// Set the schema
	self.conn = conn.With("schema", schema.CatalogSchema).(pg.PoolConn)

	// Set default options
	self.opts = opts{}

	// Get the cluster name for metrics
	var cluster schema.Cluster
	if err := self.conn.Get(context.Background(), &cluster, cluster); err != nil {
		return nil, fmt.Errorf("get cluster name: %w", err)
	} else {
		self.cluster = cluster.Name
	}

	// Apply options
	if err := self.opts.apply(opt...); err != nil {
		return nil, err
	}

	// Register metrics
	if self.metrics != nil {
		err := errors.Join(
			self.RegisterDatabaseMetrics("database"),
		)
		if err != nil {
			return nil, fmt.Errorf("register metrics: %w", err)
		}
	}

	// Return success
	return self, nil
}
