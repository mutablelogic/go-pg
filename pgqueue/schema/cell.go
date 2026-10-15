package schema

import (
	"bytes"
	"encoding/json"
	"strconv"
	"time"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

const (
	// jsonCellWidth is the most characters of JSON shown in a table cell
	jsonCellWidth = 40

	// timeCellFormat is the format of a time in a table cell
	timeCellFormat = "2006-01-02 15:04:05"
)

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// durationCell returns the duration for a table cell, or an empty string if
// it isn't set
func durationCell(d *time.Duration) string {
	if d == nil {
		return ""
	}
	return d.String()
}

// countCell returns the count for a table cell, or an empty string if it
// isn't set
func countCell(n *uint64) string {
	if n == nil {
		return ""
	}
	return strconv.FormatUint(*n, 10)
}

// timeCell returns the time, in local time, for a table cell, or an empty
// string if it isn't set
func timeCell(t *time.Time) string {
	if t == nil || t.IsZero() {
		return ""
	}
	return t.Local().Format(timeCellFormat)
}

// jsonCell returns compact JSON for a table cell, shortened to jsonCellWidth
// characters, or an empty string if it's empty or null
func jsonCell(data json.RawMessage) string {
	if len(data) == 0 || bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return ""
	}
	var value bytes.Buffer
	if err := json.Compact(&value, data); err != nil {
		value.Write(data)
	}
	cell := []rune(value.String())
	if len(cell) > jsonCellWidth {
		cell = append(cell[:jsonCellWidth-1], '…')
	}
	return string(cell)
}
