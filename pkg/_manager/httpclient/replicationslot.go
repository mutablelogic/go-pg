package httpclient

import (
	"context"

	// Packages
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	client "github.com/mutablelogic/go-client"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) ListReplicationSlots(ctx context.Context, opts ...Opt) (*schema.ReplicationSlotList, error) {
	req := client.NewRequest()

	// Apply options
	opt, err := applyOpts(opts...)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.ReplicationSlotList
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("replicationslot"), client.OptQuery(opt.Values)); err != nil {
		return nil, err
	}

	// Return the response
	return &response, nil
}

func (c *Client) GetReplicationSlot(ctx context.Context, name string) (*schema.ReplicationSlot, error) {
	req := client.NewRequest()

	// Perform request
	var response schema.ReplicationSlot
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("replicationslot", name)); err != nil {
		return nil, err
	}

	// Return the response
	return &response, nil
}

func (c *Client) CreateReplicationSlot(ctx context.Context, meta schema.ReplicationSlotMeta) (*schema.ReplicationSlot, error) {
	req, err := client.NewJSONRequest(meta)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.ReplicationSlot
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("replicationslot")); err != nil {
		return nil, err
	}

	// Return the response
	return &response, nil
}

func (c *Client) DeleteReplicationSlot(ctx context.Context, name string) error {
	return c.DoWithContext(ctx, client.MethodDelete, nil, client.OptPath("replicationslot", name))
}
