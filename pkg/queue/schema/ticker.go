package schema

import (
	"encoding/json"
	"strings"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

type TickerName string

type TickerMeta struct {
	Ticker   string          `json:"ticker" arg:"" help:"Ticker name"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Interval *time.Duration  `json:"interval,omitempty" help:"Interval (default 1 minute)"`
}

type Ticker struct {
	TickerMeta
	Namespace string     `json:"namespace,omitempty" help:"Namespace"`
	Ts        *time.Time `json:"timestamp,omitempty"`
}

type TickerListRequest struct {
	pg.OffsetLimit
}

type TickerList struct {
	TickerListRequest
	Count uint64   `json:"count"`
	Body  []Ticker `json:"body,omitempty"`
}

type TickerNext struct{}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (t Ticker) String() string {
	return stringify(t)
}

func (t TickerMeta) String() string {
	return stringify(t)
}

func (t TickerList) String() string {
	return stringify(t)
}

////////////////////////////////////////////////////////////////////////////////
// SELECTOR

func (q TickerName) Select(bind *pg.Bind, op pg.Op) (string, error) {
	// Set ticker name
	if ticker, err := q.tickerName(); err != nil {
		return "", err
	} else {
		bind.Set("id", ticker)
	}

	switch op {
	case pg.Get:
		return bind.Replace("${pgqueue.ticker_get}"), nil
	case pg.Update:
		return bind.Replace("${pgqueue.ticker_patch}"), nil
	case pg.Delete:
		return bind.Replace("${pgqueue.ticker_delete}"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("Unsupported TickerName operation %q", op)
	}
}

func (t TickerListRequest) Select(bind *pg.Bind, op pg.Op) (string, error) {
	t.OffsetLimit.Bind(bind, TickerListLimit)

	switch op {
	case pg.List:
		return bind.Replace("${pgqueue.ticker_list}"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("Unsupported TickerListRequest operation %q", op)
	}
}

func (t TickerNext) Select(bind *pg.Bind, op pg.Op) (string, error) {
	switch op {
	case pg.Get:
		return bind.Replace("${pgqueue.ticker_next}"), nil
	default:
		return "", httpresponse.ErrInternalError.Withf("Unsupported TickerNext operation %q", op)
	}
}

////////////////////////////////////////////////////////////////////////////////
// READER

func (r *Ticker) Scan(row pg.Row) error {
	return row.Scan(&r.Ticker, &r.Interval, &r.Namespace, &r.Payload, &r.Ts)
}

// TickerList
func (l *TickerList) Scan(row pg.Row) error {
	var ticker Ticker
	if err := ticker.Scan(row); err != nil {
		return err
	}
	l.Body = append(l.Body, ticker)
	return nil
}

// TickerListCount
func (l *TickerList) ScanCount(row pg.Row) error {
	return row.Scan(&l.Count)
}

////////////////////////////////////////////////////////////////////////////////
// WRITER

func (w TickerMeta) Insert(bind *pg.Bind) (string, error) {
	// Set ticker
	if ticker, err := TickerName(w.Ticker).tickerName(); err != nil {
		return "", err
	} else {
		bind.Set("id", ticker)
	}

	// Return the query
	return bind.Replace("${pgqueue.ticker_insert}"), nil
}

func (w TickerMeta) Update(bind *pg.Bind) error {
	bind.Del("patch")

	// Check for id
	if !bind.Has("id") {
		return httpresponse.ErrBadRequest.With("Missing id parameter")
	}

	// Set interval
	if w.Interval != nil {
		bind.Append("patch", `"interval" = `+bind.Set("interval", w.Interval))
	}

	// Set name
	if w.Ticker != "" {
		if name, err := TickerName(w.Ticker).tickerName(); err != nil {
			return err
		} else {
			bind.Append("patch", `"ticker" = `+bind.Set("ticker", name))
		}
	}

	// Payload
	if len(w.Payload) > 0 {
		bind.Append("patch", `payload = `+bind.Set("payload", string(w.Payload)))
	}

	// Set patch
	if patch := bind.Join("patch", ","); patch == "" {
		return httpresponse.ErrBadRequest.With("No patch values")
	} else {
		bind.Set("patch", patch)
	}

	// Return success
	return nil
}

// Normalize ticker name
func (q TickerName) tickerName() (string, error) {
	if name := strings.ToLower(strings.TrimSpace(string(q))); name == "" {
		return "", httpresponse.ErrBadRequest.With("Missing ticker name")
	} else if !types.IsIdentifier(name) {
		return "", httpresponse.ErrBadRequest.Withf("Invalid ticker name: %q", name)
	} else {
		return name, nil
	}
}
