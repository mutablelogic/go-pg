package pg

import (
	"fmt"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type OffsetLimit struct {
	Offset uint64  `json:"offset,omitempty" help:"Offset within the result set" jsonschema:"Offset for pagination" example:"0"`
	Limit  *uint64 `json:"limit,omitempty" help:"Number of items to return" jsonschema:"Limit for pagination" example:"100"`
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Bind sets the offset and limit SQL fragment on the bind object.
func (r *OffsetLimit) Bind(bind *Bind, max uint64) {
	if (r.Limit != nil && *r.Limit > max) || r.Limit == nil {
		r.Limit = &max
	}
	if r.Offset != 0 && r.Limit != nil {
		bind.Set("offsetlimit", fmt.Sprintf("LIMIT %v OFFSET %v", *r.Limit, r.Offset))
	} else if r.Limit != nil {
		bind.Set("offsetlimit", fmt.Sprintf("LIMIT %v", *r.Limit))
	} else if r.Offset != 0 {
		bind.Set("offsetlimit", fmt.Sprintf("OFFSET %v", r.Offset))
	} else {
		bind.Set("offsetlimit", "")
	}
}

// Clamp restricts the limit to the maximum length.
func (r *OffsetLimit) Clamp(len uint64) {
	if r.Limit != nil {
		*r.Limit = min(*r.Limit, len)
	}
}
