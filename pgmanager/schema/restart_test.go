package schema_test

import (
	"strings"
	"testing"
	"time"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_ReadPostmasterPID(t *testing.T) {
	file := "57\n/var/lib/postgresql/data\n1763027896\n5432\n/var/run/postgresql\n*\n  1234567  0\nready   \n"
	pid, err := schema.ReadPostmasterPID(strings.NewReader(file))
	require.NoError(t, err)
	assert.Equal(t, 57, pid.Pid)
	assert.Equal(t, "/var/lib/postgresql/data", pid.DataDirectory)
	assert.Equal(t, int64(1763027896), pid.Started.Unix())

	// Too short, or without a process, isn't one
	_, err = schema.ReadPostmasterPID(strings.NewReader("57\n/var/lib/postgresql/data\n"))
	assert.Error(t, err)
	_, err = schema.ReadPostmasterPID(strings.NewReader("x\n/data\n1763027896\n"))
	assert.Error(t, err)
}

func Test_PostmasterPID_Matches(t *testing.T) {
	started := time.Unix(1763027896, 0)
	pid := schema.PostmasterPID{Pid: 57, DataDirectory: "/data", Started: started}

	// The server starts a little after its file's second
	assert.NoError(t, pid.Matches(schema.Postmaster{DataDirectory: "/data", Started: started.Add(350 * time.Millisecond)}))

	// Another data directory, or a server which started at another time,
	// such as a file left by a server which has stopped, isn't the server's
	assert.Error(t, pid.Matches(schema.Postmaster{DataDirectory: "/other", Started: started}))
	assert.Error(t, pid.Matches(schema.Postmaster{DataDirectory: "/data", Started: started.Add(time.Hour)}))
}
