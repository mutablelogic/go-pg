package manager_test

import (
	"testing"

	// Packages
	harness "github.com/mutablelogic/go-pg/pgmanager/test"
)

func TestMain(m *testing.M) {
	harness.Main(m, nil)
}

func TestManagerHarness(t *testing.T) {
	mgr, _ := harness.Begin(t)
	defer harness.End(t)

	if mgr == nil {
		t.Fatal("manager is nil")
	}
}
