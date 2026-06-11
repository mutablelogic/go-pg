package httpclient

import (
	"context"

	// Packages
	client "github.com/mutablelogic/go-client"
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Client) ListReplicationSlots(ctx context.Context, req schema.ReplicationSlotListRequest) (*schema.ReplicationSlotList, error) {
	var response schema.ReplicationSlotList
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("replication-slot"), client.OptQuery(req.Query())); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) GetReplicationSlot(ctx context.Context, name string) (*schema.ReplicationSlot, error) {
	var response schema.ReplicationSlot
	if err := c.DoWithContext(ctx, client.MethodGet, &response, client.OptPath("replication-slot", name)); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) CreateReplicationSlot(ctx context.Context, meta schema.ReplicationSlotMeta) (*schema.ReplicationSlot, error) {
	req, err := client.NewJSONRequest(meta)
	if err != nil {
		return nil, err
	}

	// Perform request
	var response schema.ReplicationSlot
	if err := c.DoWithContext(ctx, req, &response, client.OptPath("replication-slot")); err != nil {
		return nil, err
	}

	// Return the responses
	return types.Ptr(response), nil
}

func (c *Client) DeleteReplicationSlot(ctx context.Context, name string) error {
	return c.DoWithContext(ctx, client.MethodDelete, nil, client.OptPath("replication-slot", name))
}
