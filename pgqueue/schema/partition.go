package schema

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

type PartitionMeta struct {
	Partition string `json:"partition" db:"partition"`
	Start     uint64 `json:"start" db:"start"`
	End       uint64 `json:"end" db:"end"`
}

type PartitionName string

type PartitionSeq uint64

type Partition struct {
	PartitionMeta
	Count uint64 `json:"count" db:"count"`
}

type PartitionListRequest struct{}

type PartitionSeqRequest struct{}

type PartitionList struct {
	Body []Partition `json:"body,omitempty"`
}

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

var partitionBoundsPattern = regexp.MustCompile(`FROM \('?([0-9]+)'?\) TO \('?([0-9]+)'?\)`)

///////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (p Partition) String() string {
	return types.Stringify(p)
}

func (p PartitionMeta) String() string {
	return types.Stringify(p)
}

func (p PartitionName) String() string {
	return types.Stringify(p)
}

func (p PartitionSeq) String() string {
	return types.Stringify(p)
}

func (p PartitionListRequest) String() string {
	return types.Stringify(p)
}

func (p PartitionSeqRequest) String() string {
	return types.Stringify(p)
}

func (p PartitionList) String() string {
	return types.Stringify(p)
}

///////////////////////////////////////////////////////////////////////////////
// READER

func (p *Partition) Scan(row pg.Row) error {
	var bounds string
	if err := row.Scan(&p.PartitionMeta.Partition, &bounds, &p.Count); err != nil {
		return err
	}

	matches := partitionBoundsPattern.FindStringSubmatch(bounds)
	if len(matches) != 3 {
		return fmt.Errorf("parse partition bounds %q", bounds)
	}

	start, err := strconv.ParseUint(matches[1], 10, 64)
	if err != nil {
		return fmt.Errorf("parse partition start %q: %w", matches[1], err)
	}
	end, err := strconv.ParseUint(matches[2], 10, 64)
	if err != nil {
		return fmt.Errorf("parse partition end %q: %w", matches[2], err)
	}

	p.Start = start
	p.End = end

	return nil
}

func (l *PartitionList) Scan(row pg.Row) error {
	var partition Partition
	if err := partition.Scan(row); err != nil {
		return err
	}
	l.Body = append(l.Body, partition)
	return nil
}

func (p *PartitionSeq) Scan(row pg.Row) error {
	var seq uint64
	if err := row.Scan(&seq); err != nil {
		return err
	}
	*p = PartitionSeq(seq)
	return nil
}

///////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (p PartitionListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	switch op {
	case pg.List:
		return bind.Query("pgqueue.partition_list"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported PartitionListRequest operation %q", op)
	}
}

func (p PartitionName) Select(bind *pg.Bind, op pg.Op) (string, error) {
	name, err := partitionName(string(p))
	if err != nil {
		return "", err
	}
	bind.Set("partition", name)

	switch op {
	case pg.Delete:
		return bind.Query("pgqueue.partition_drop"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported PartitionName operation %q", op)
	}
}

func (p PartitionSeqRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	switch op {
	case pg.Get:
		return bind.Query("pgqueue.partition_seq"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported PartitionSeqRequest operation %q", op)
	}
}

///////////////////////////////////////////////////////////////////////////////
// WRITER

func (p Partition) Insert(bind *pg.Bind) (string, error) {
	return p.PartitionMeta.Insert(bind)
}

func (p PartitionMeta) Insert(bind *pg.Bind) (string, error) {
	partition, err := partitionName(p.Partition)
	if err != nil {
		return "", err
	}
	if p.End <= p.Start {
		return "", httpresponse.ErrBadRequest.With("invalid partition range")
	}

	bind.Set("partition", partition)
	bind.Set("start", p.Start)
	bind.Set("end", p.End)

	return bind.Query("pgqueue.partition_create"), nil
}

func (p Partition) Update(bind *pg.Bind) error {
	return p.PartitionMeta.Update(bind)
}

func (p PartitionMeta) Update(bind *pg.Bind) error {
	return httpresponse.ErrBadRequest.With("partition update not supported")
}

func partitionName(name string) (string, error) {
	if name = strings.ToLower(strings.TrimSpace(name)); !types.IsIdentifier(name) {
		return "", httpresponse.ErrBadRequest.Withf("invalid partition name: %q", name)
	} else {
		return name, nil
	}
}
