package schema

import (
	"encoding/json"
	"net/url"
	"strings"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	tui "github.com/mutablelogic/go-server/pkg/tui"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type TickerName string

type TickerMeta struct {
	Payload  json.RawMessage `json:"payload,omitempty"`
	Interval *time.Duration  `json:"interval,omitempty" help:"Interval (default 1 minute)"`
}

type Ticker struct {
	Ticker string `json:"ticker" arg:"" help:"Ticker name"`
	TickerMeta
	LastAt *time.Time `json:"last_at,omitempty"`
}

type TickerListRequest struct {
	pg.OffsetLimit
}

type TickerList struct {
	TickerListRequest
	Count uint64   `json:"count"`
	Body  []Ticker `json:"body,omitempty"`
}

// TickerNext gets the next due ticker of the tickers with the names
type TickerNext struct {
	Tickers []string `json:"tickers,omitempty"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (t Ticker) String() string {
	return types.Stringify(t)
}

func (t TickerMeta) String() string {
	return types.Stringify(t)
}

func (t TickerListRequest) String() string {
	return types.Stringify(t)
}

func (t TickerList) String() string {
	return types.Stringify(t)
}

func (t TickerNext) String() string {
	return types.Stringify(t)
}

////////////////////////////////////////////////////////////////////////////////
// TABLE

func (t Ticker) Header() []string {
	return []string{"Ticker", "Interval", "Last Run", "Payload"}
}

func (t Ticker) Width(col int) int {
	return 0
}

func (t Ticker) Cell(col int) tui.CellText {
	switch col {
	case 0:
		return t.Ticker
	case 1:
		return durationCell(t.Interval)
	case 2:
		return timeCell(t.LastAt)
	case 3:
		return jsonCell(t.Payload)
	default:
		return ""
	}
}

////////////////////////////////////////////////////////////////////////////////
// QUERY

// Query returns the request as URL query parameters
func (t TickerListRequest) Query() url.Values {
	values := url.Values{}
	t.OffsetLimit.Query(values)
	return values
}

////////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (t TickerName) Select(bind *pg.Bind, op pg.Op) (string, error) {
	if name, err := t.Validate(); err != nil {
		return "", err
	} else {
		bind.Set("id", name)
	}

	switch op {
	case pg.Get:
		return bind.Query("pgqueue.ticker_get"), nil
	case pg.Update:
		return bind.Query("pgqueue.ticker_patch"), nil
	case pg.Delete:
		return bind.Query("pgqueue.ticker_delete"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported TickerName operation %q", op)
	}
}

func (t TickerListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	t.OffsetLimit.Bind(bind, TickerListLimit)

	switch op {
	case pg.List:
		return bind.Query("pgqueue.ticker_list"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported TickerListRequest operation %q", op)
	}
}

func (t TickerNext) Select(bind *pg.Bind, op pg.Op) (string, error) {
	tickers := make([]string, 0, len(t.Tickers))
	for _, ticker := range t.Tickers {
		name, err := TickerName(ticker).Validate()
		if err != nil {
			return "", err
		}
		tickers = append(tickers, name)
	}
	bind.Set("tickers", tickers)

	switch op {
	case pg.Get:
		return bind.Query("pgqueue.ticker_next"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("unsupported TickerNext operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (t *Ticker) Scan(row pg.Row) error {
	return row.Scan(&t.Ticker, &t.Payload, &t.Interval, &t.LastAt)
}

func (l *TickerList) Scan(row pg.Row) error {
	var ticker Ticker
	if err := ticker.Scan(row); err != nil {
		return err
	}
	l.Body = append(l.Body, ticker)
	return nil
}

func (l *TickerList) ScanCount(row pg.Row) error {
	return row.Scan(&l.Count)
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

func (t TickerMeta) Insert(bind *pg.Bind) (string, error) {
	if !bind.Has("id") {
		return "", httpresponse.ErrBadRequest.With("missing id parameter")
	}

	name, _ := bind.Get("id").(string)
	if ticker, err := TickerName(name).Validate(); err != nil {
		return "", err
	} else {
		bind.Set("ticker", ticker)
	}

	return bind.Query("pgqueue.ticker_insert"), nil
}

func (t TickerMeta) Update(bind *pg.Bind) error {
	bind.Del("patch")

	if t.Interval != nil {
		// A ticker with no interval would always be due
		if *t.Interval <= 0 {
			return httpresponse.ErrBadRequest.Withf("ticker interval must be positive: %v", *t.Interval)
		}
		bind.Append("patch", `"interval" = `+bind.Set("interval", t.Interval))
	}
	if len(t.Payload) > 0 {
		bind.Append("patch", `"payload" = CAST(`+bind.Set("payload", string(t.Payload))+` AS JSONB)`)
	}

	if patch := bind.Join("patch", ", "); patch == "" {
		return httpresponse.ErrBadRequest.With("no patch values")
	} else {
		bind.Set("patch", patch)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////
// VALIDATION HELPERS

func (t TickerName) Validate() (string, error) {
	// Reserved ticker names
	if string(t) == DefaultMaintenanceTickerName || string(t) == DefaultCleanupTickerName {
		return string(t), nil
	}
	// User-defined ticker names must be valid identifiers
	if name := strings.ToLower(strings.TrimSpace(string(t))); !types.IsIdentifier(name) {
		return "", httpresponse.ErrBadRequest.Withf("invalid ticker name: %q", name)
	} else {
		return name, nil
	}
}
