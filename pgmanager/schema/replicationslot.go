package schema

import (
	"fmt"
	"net/url"
	"strings"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	"github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

// ReplicationSlotName is used for get/delete operations
type ReplicationSlotName string

// ReplicationSlotMeta contains parameters for creating a replication slot
type ReplicationSlotMeta struct {
	Name      string `json:"name" arg:"" help:"Name of the replication slot"`
	Type      string `json:"type" enum:"physical,logical" required:"" help:"Type of the replication slot"`
	Plugin    string `json:"plugin,omitempty" help:"Plugin for logical replication slots (e.g., pgoutput)"`
	Database  string `json:"database,omitempty" help:"Database for logical replication slots"`
	Temporary bool   `json:"temporary,omitempty" negatable:"" help:"If true, slot will be dropped on disconnect; logical slots only"`
	TwoPhase  bool   `json:"two_phase,omitempty"  negatable:"" help:"Logical slots only, allows PREPARE/COMMIT semantics for streaming transactions"`
}

// ReplicationSlot represents a replication slot with its status
type ReplicationSlot struct {
	ReplicationSlotMeta

	// Combined status: inactive, streaming, catchup, lost
	Status string `json:"status"`

	// Connected replica info (when streaming/catchup)
	ClientAddr string   `json:"client_addr,omitempty"`
	LagBytes   *int64   `json:"lag_bytes,omitempty"`
	LagMs      *float64 `json:"lag_ms,omitempty"`
}

// ReplicationSlotListRequest contains parameters for listing replication slots
type ReplicationSlotListRequest struct {
	pg.OffsetLimit
}

// ReplicationSlotList is a list of replication slots with a total count
type ReplicationSlotList struct {
	ReplicationSlotListRequest
	Count uint64            `json:"count"`
	Body  []ReplicationSlot `json:"body,omitempty"`
}

///////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (s ReplicationSlot) String() string {
	return types.Stringify(s)
}

func (s ReplicationSlotMeta) String() string {
	return types.Stringify(s)
}

func (l ReplicationSlotList) String() string {
	return types.Stringify(l)
}

////////////////////////////////////////////////////////////////////////////////
// TABLE

func (r ReplicationSlot) Header() []string {
	return []string{"Name", "Type", "Plugin", "Database", "Temporary", "TwoPhase", "Status", "ClientAddr", "LagBytes", "LagMs"}
}

func (r ReplicationSlot) Width(col int) int {
	return 0
}

func (r ReplicationSlot) Cell(col int) string {
	switch col {
	case 0:
		return r.Name
	case 1:
		return r.Type
	case 2:
		return r.Plugin
	case 3:
		return r.Database
	case 4:
		return fmt.Sprint(r.Temporary)
	case 5:
		return fmt.Sprint(r.TwoPhase)
	case 6:
		return r.Status
	case 7:
		return r.ClientAddr
	case 8:
		if r.LagBytes != nil {
			return fmt.Sprint(*r.LagBytes)
		}
		return ""
	case 9:
		if r.LagMs != nil {
			return fmt.Sprintf("%.2f", *r.LagMs)
		}
		return ""
	default:
		return ""
	}
}

////////////////////////////////////////////////////////////////////////////////
// QUERY

func (d ReplicationSlotListRequest) Query() url.Values {
	q := url.Values{}
	if d.Offset > 0 {
		q.Set("offset", fmt.Sprint(d.Offset))
	}
	if d.Limit != nil {
		q.Set("limit", fmt.Sprint(types.Value(d.Limit)))
	}
	return q
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

func (r *ReplicationSlotListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
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
