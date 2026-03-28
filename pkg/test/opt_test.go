package test

import (
	"strings"
	"testing"

	// Packages
	nat "github.com/docker/go-connections/nat"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
	testcontainers "github.com/testcontainers/testcontainers-go"
	wait "github.com/testcontainers/testcontainers-go/wait"
)

func Test_Opt_001(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	o := opts{req: testcontainers.ContainerRequest{WaitingFor: wait.ForAll()}}

	require.NoError(OptEntrypoint("/bin/sh", "-c")(&o))
	require.NoError(OptCommand([]string{"echo", "hello"})(&o))
	require.NoError(OptEnv("A", "1")(&o))
	require.NoError(OptPorts("3389/tcp", "389/tcp")(&o))
	require.NoError(OptFile(testcontainers.ContainerFile{
		Reader:            strings.NewReader("echo bootstrap\n"),
		ContainerFilePath: "/bootstrap.sh",
		FileMode:          0o755,
	})(&o))
	require.NoError(OptWaitLog("server ready")(&o))

	assert.Equal([]string{"/bin/sh", "-c"}, o.req.Entrypoint)
	assert.Equal([]string{"echo", "hello"}, o.req.Cmd)
	assert.Equal("1", o.req.Env["A"])
	assert.Equal([]string{"3389/tcp", "389/tcp"}, o.req.ExposedPorts)
	if assert.Len(o.req.Files, 1) {
		assert.Equal("/bootstrap.sh", o.req.Files[0].ContainerFilePath)
		assert.Equal(int64(0o755), o.req.Files[0].FileMode)
	}

	multi, ok := o.req.WaitingFor.(*wait.MultiStrategy)
	require.True(ok)
	assert.Len(multi.Strategies, 3)
	assert.IsType(wait.ForListeningPort(nat.Port("3389/tcp")), multi.Strategies[0])
	assert.IsType(wait.ForListeningPort(nat.Port("389/tcp")), multi.Strategies[1])
	assert.IsType(wait.ForLog("server ready"), multi.Strategies[2])
}

func Test_Opt_002(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	o := opts{req: testcontainers.ContainerRequest{WaitingFor: wait.ForAll()}}
	require.NoError(OptFiles(
		testcontainers.ContainerFile{ContainerFilePath: "/one", FileMode: 0o644},
		testcontainers.ContainerFile{ContainerFilePath: "/two", FileMode: 0o600},
	)(&o))

	if assert.Len(o.req.Files, 2) {
		assert.Equal("/one", o.req.Files[0].ContainerFilePath)
		assert.Equal("/two", o.req.Files[1].ContainerFilePath)
	}
}
