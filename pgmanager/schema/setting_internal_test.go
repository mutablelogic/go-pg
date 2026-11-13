package schema

import (
	"testing"

	// Packages
	types "github.com/mutablelogic/go-server/pkg/types"
	assert "github.com/stretchr/testify/assert"
)

func Test_SettingFileError_RestartOnly(t *testing.T) {
	maxConnections := &Setting{Type: "integer", Context: "postmaster", Min: types.Ptr("1"), Max: types.Ptr("262143"), PendingRestart: true}
	sharedBuffers := &Setting{Type: "integer", Context: "postmaster", Min: types.Ptr("16"), Max: types.Ptr("1073741823"), PendingRestart: true}
	walLevel := &Setting{Type: "enum", Context: "postmaster", Values: []string{"minimal", "replica", "logical"}, PendingRestart: true}
	ssl := &Setting{Type: "bool", Context: "postmaster", PendingRestart: true}

	tests := []struct {
		name    string
		value   string
		setting *Setting
		want    bool
	}{
		// A value of its type, within its limits, or with a unit, waits for
		// a restart
		{"integer", "120", maxConnections, true},
		{"unit", "256MB", sharedBuffers, true},
		{"enum", "Logical", walLevel, true},
		{"bool", "on", ssl, true},

		// A value which isn't, is beyond its limits, or of a setting which
		// doesn't wait for a restart, or needs none, or is unknown, is an
		// error
		{"not a number", "lots", maxConnections, false},
		{"beyond its limits", "0", maxConnections, false},
		{"not a whole number", "1.5", maxConnections, false},
		{"not a value", "archive", walLevel, false},
		{"not a bool", "maybe", ssl, false},
		{"not pending", "120", &Setting{Type: "integer", Context: "postmaster"}, false},
		{"needs no restart", "8MB", &Setting{Type: "integer", Context: "user", PendingRestart: true}, false},
		{"unknown", "1", nil, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e := SettingFileError{Name: "x", Error: "setting could not be applied", value: test.value, setting: test.setting}
			assert.Equal(t, test.want, e.RestartOnly())
		})
	}
}
