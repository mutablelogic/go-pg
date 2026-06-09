package schema

import (
	"encoding/json"
	"strings"

	// Packages
	pg "github.com/mutablelogic/go-pg"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// ReplicationSlotName is used for get/delete operations
type ReplicationSlotName string

// ReplicationSlotMeta contains parameters for creating a replication slot
type ReplicationSlotMeta struct {
	Name      string `json:"name"`
	Type      string `json:"type"`               // physical, logical
	Plugin    string `json:"plugin,omitempty"`   // required for logical (e.g., pgoutput)
	Database  string `json:"database,omitempty"` // required for logical
	Temporary bool   `json:"temporary,omitempty"`
	TwoPhase  bool   `json:"two_phase,omitempty"` // PG14+
}

// ReplicationSlot represents a replication slot with its status
type ReplicationSlot struct {
	ReplicationSlotMeta

	// Combined status: inactive, streaming, catchup, lost
	Status string `json:"status"`

	// Connected replica info (when streaming/catchup)
	ClientAddr string   `json:"client_addr,omitempty"`
	LagBytes   *int64   `json:"lag_bytes,omitempty"`
	LagMs      *float64 `json:"lag_ms"`
}

// ReplicationSlotListRequest contains parameters for listing replication slots
type ReplicationSlotListRequest struct {
	pg.OffsetLimit
}

// ReplicationSlotList is a list of replication slots with a total count
type ReplicationSlotList struct {
	Count uint64            `json:"count"`
	Body  []ReplicationSlot `json:"body,omitempty"`
}

///////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (s ReplicationSlot) String() string {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (s ReplicationSlotMeta) String() string {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (l ReplicationSlotList) String() string {
	data, err := json.MarshalIndent(l, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

///////////////////////////////////////////////////////////////////////////////
// VALIDATE

func (m ReplicationSlotMeta) Validate() error {
	name := strings.TrimSpace(m.Name)
	if name == "" {
		return pg.ErrBadParameter.With("name is required")
	}
	if strings.HasPrefix(name, reservedPrefix) {
		return pg.ErrBadParameter.Withf("name cannot start with %q", reservedPrefix)
	}

	slotType := strings.ToLower(m.Type)
	if slotType != "physical" && slotType != "logical" {
		return pg.ErrBadParameter.With("type must be 'physical' or 'logical'")
	}

	if slotType == "logical" {
		if strings.TrimSpace(m.Plugin) == "" {
			return pg.ErrBadParameter.With("plugin is required for logical slots")
		}
	}

	return nil
}

///////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (n ReplicationSlotName) name() (string, error) {
	name := strings.TrimSpace(string(n))
	if name == "" {
		return "", pg.ErrBadParameter.With("name is required")
	}
	if strings.HasPrefix(name, reservedPrefix) {
		return "", pg.ErrBadParameter.Withf("name cannot start with %q", reservedPrefix)
	}
	return name, nil
}

func (n ReplicationSlotName) Select(bind *pg.Bind, op pg.Op) (string, error) {
	name, err := n.name()
	if err != nil {
		return "", err
	}
	bind.Set("name", name)

	switch op {
	case pg.Get:
		return replicationSlotGet, nil
	case pg.Delete:
		return replicationSlotDelete, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported ReplicationSlotName operation %q", op)
	}
}

func (r ReplicationSlotListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	bind.Set("where", "")
	bind.Set("orderby", "ORDER BY name ASC")

	r.OffsetLimit.Bind(bind, ReplicationSlotListLimit)

	switch op {
	case pg.List:
		return replicationSlotList, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported ReplicationSlotListRequest operation %q", op)
	}
}

///////////////////////////////////////////////////////////////////////////////
// WRITER

func (m ReplicationSlotMeta) Insert(bind *pg.Bind) (string, error) {
	if err := m.Validate(); err != nil {
		return "", err
	}

	bind.Set("name", strings.TrimSpace(m.Name))
	bind.Set("temporary", m.Temporary)
	bind.Set("two_phase", m.TwoPhase)

	if strings.ToLower(m.Type) == "logical" {
		bind.Set("plugin", strings.TrimSpace(m.Plugin))
		return replicationSlotCreateLogical, nil
	}
	return replicationSlotCreatePhysical, nil
}

func (m ReplicationSlotMeta) Update(_ *pg.Bind) error {
	return pg.ErrNotImplemented.With("replication slots cannot be updated")
}

///////////////////////////////////////////////////////////////////////////////
// READER

func (s *ReplicationSlot) Scan(row pg.Row) error {
	var lagBytes, lagMs *float64
	err := row.Scan(
		&s.Name, &s.Type, &s.Plugin, &s.Database,
		&s.Temporary, &s.TwoPhase,
		&s.Status, &s.ClientAddr,
		&lagBytes, &lagMs,
	)
	if err != nil {
		return err
	}

	// Convert lag values
	if lagBytes != nil {
		v := int64(*lagBytes)
		s.LagBytes = &v
	}
	if lagMs != nil {
		s.LagMs = lagMs
	}

	return nil
}

func (l *ReplicationSlotList) Scan(row pg.Row) error {
	var slot ReplicationSlot
	if err := slot.Scan(row); err != nil {
		return err
	}
	l.Body = append(l.Body, slot)
	return nil
}

func (l *ReplicationSlotList) ScanCount(row pg.Row) error {
	return row.Scan(&l.Count)
}

///////////////////////////////////////////////////////////////////////////////
// QUERIES

const (
	replicationSlotSelect = `
		SELECT
			s.slot_name AS name,
			s.slot_type AS type,
			COALESCE(s.plugin, '') AS plugin,
			COALESCE(s.database, '') AS database,
			COALESCE(s.temporary, false) AS temporary,
			COALESCE(s.two_phase, false) AS two_phase,
			CASE
				WHEN s.wal_status = 'lost' THEN 'lost'
				WHEN s.active AND r.replay_lag IS NOT NULL AND r.replay_lag > interval '0' THEN 'catchup'
				WHEN s.active THEN 'streaming'
				ELSE 'inactive'
			END AS status,
			COALESCE(host(r.client_addr), '') AS client_addr,
			CASE 
				WHEN r.replay_lsn IS NOT NULL THEN 
					(pg_current_wal_lsn() - r.replay_lsn)::float8
				ELSE NULL 
			END AS lag_bytes,
			CASE 
				WHEN r.replay_lag IS NOT NULL THEN 
					EXTRACT(EPOCH FROM r.replay_lag) * 1000
				WHEN r.replay_lsn IS NOT NULL THEN 
					0
				ELSE NULL 
			END AS lag_ms
		FROM
			pg_replication_slots s
		LEFT JOIN
			pg_stat_replication r ON s.active_pid = r.pid
	`
	replicationSlotList           = `WITH q AS (` + replicationSlotSelect + `) SELECT * FROM q ${where} ${orderby}`
	replicationSlotGet            = replicationSlotSelect + ` WHERE s.slot_name = @name`
	replicationSlotCreatePhysical = `SELECT pg_create_physical_replication_slot(@name, true, @temporary)`
	replicationSlotCreateLogical  = `SELECT pg_create_logical_replication_slot(@name, @plugin, @temporary, @two_phase)`
	replicationSlotDelete         = `SELECT pg_drop_replication_slot(@name)`
)
