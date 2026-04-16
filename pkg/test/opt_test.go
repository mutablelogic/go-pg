package test

import (
	"strings"
	"testing"

	// Packages
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

	strategy0, ok := multi.Strategies[0].(*wait.HostPortStrategy)
	require.True(ok)
	assert.Equal("3389/tcp", strategy0.Port)

	strategy1, ok := multi.Strategies[1].(*wait.HostPortStrategy)
	require.True(ok)
	assert.Equal("389/tcp", strategy1.Port)

	strategy2, ok := multi.Strategies[2].(*wait.LogStrategy)
	require.True(ok)
	assert.Equal("server ready", strategy2.Log)
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

func Test_Opt_003(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(
		"postgres://postgres:password@localhost:5432/httphandler.test?sslmode=disable",
		postgresURL("postgres", "password", "httphandler.test", "localhost", "5432/tcp"),
	)
}
