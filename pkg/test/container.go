package test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	// Packages
	testcontainers "github.com/testcontainers/testcontainers-go"
	wait "github.com/testcontainers/testcontainers-go/wait"
)

//////////////////////////////////////////////////////////////////////////////
// TYPES

type Container struct {
	testcontainers.Container `json:"-"`
	Env                      map[string]string `json:"env"`
	MappedPorts              map[string]string `json:"mapped_ports"`
}

//////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

// NewContainer creates a new container with the given name and image.
func NewContainer(ctx context.Context, name, image string, opt ...Opt) (*Container, error) {
	// The name has _unixtime appended to it
	name = fmt.Sprintf("%s_%v", name, time.Now().Unix())

	// Apply the options
	var o opts
	o.req = testcontainers.ContainerRequest{
		Name:       name,
		Image:      image,
		WaitingFor: wait.ForAll(),
	}
	for _, opt := range opt {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}

	// If there are no wait strategies, then wait for container exit
	if o.req.WaitingFor.(*wait.MultiStrategy).Strategies == nil {
		o.req.WaitingFor = wait.ForExit()
	}

	// Create the container, wait until started
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: o.req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	var response Container
	response.Container = container
	response.Env = o.req.Env
	response.MappedPorts = make(map[string]string, len(o.req.ExposedPorts))

	for _, port := range o.req.ExposedPorts {
		mappedPort, err := container.MappedPort(ctx, port)
		if err != nil {
			return nil, err
		}
		response.MappedPorts[port] = mappedPort.Port()
	}

	// Return success
	return &response, nil
}

func (c *Container) Close(ctx context.Context) error {
	return c.Container.Terminate(ctx)
}

//////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (c *Container) String() string {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

//////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

func (c *Container) GetEnv(name string) (string, error) {
	if c.Env != nil {
		if value, ok := c.Env[name]; ok {
			return value, nil
		}
	}
	return "", fmt.Errorf("env: %q not found", name)
}

func (c *Container) GetPort(name string) (string, error) {
	if c.MappedPorts != nil {
		if value, ok := c.MappedPorts[name]; ok {
			return value, nil
		}
	}
	return "", fmt.Errorf("ports: %q not found", name)
}
