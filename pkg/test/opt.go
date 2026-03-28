package test

import (
	"fmt"

	// Packages
	nat "github.com/docker/go-connections/nat"
	testcontainers "github.com/testcontainers/testcontainers-go"
	wait "github.com/testcontainers/testcontainers-go/wait"

	// Anonymous imports
	_ "github.com/jackc/pgx/v5/stdlib"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Opt mutates a container request before it is started.
type Opt func(*opts) error

type opts struct {
	req testcontainers.ContainerRequest
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// OptCommand sets the container command.
func OptCommand(cmd []string) Opt {
	return func(o *opts) error {
		o.req.Cmd = cmd
		return nil
	}
}

// OptEntrypoint sets the container entrypoint.
func OptEntrypoint(entrypoint ...string) Opt {
	return func(o *opts) error {
		o.req.Entrypoint = entrypoint
		return nil
	}
}

// OptEnv sets a single container environment variable.
func OptEnv(name, value string) Opt {
	return func(o *opts) error {
		if o.req.Env == nil {
			o.req.Env = make(map[string]string)
		}
		o.req.Env[name] = value
		return nil
	}
}

// OptPorts exposes one or more container ports and waits for each requested port.
func OptPorts(ports ...string) Opt {
	return func(o *opts) error {
		o.req.ExposedPorts = ports
		for _, port := range ports {
			o.appendWaitStrategy(wait.ForListeningPort(nat.Port(port)))
		}
		return nil
	}
}

// OptFile copies a single file into the container before startup.
func OptFile(file testcontainers.ContainerFile) Opt {
	return func(o *opts) error {
		o.req.Files = append(o.req.Files, file)
		return nil
	}
}

// OptFiles copies multiple files into the container before startup.
func OptFiles(files ...testcontainers.ContainerFile) Opt {
	return func(o *opts) error {
		o.req.Files = append(o.req.Files, files...)
		return nil
	}
}

// OptWait appends a custom wait strategy to the container request.
func OptWait(strategy wait.Strategy) Opt {
	return func(o *opts) error {
		o.appendWaitStrategy(strategy)
		return nil
	}
}

// OptWaitLog waits for a specific log line before considering the container ready.
func OptWaitLog(log string) Opt {
	return OptWait(wait.ForLog(log))
}

// OptPostgres configures a PostgreSQL container and adds SQL readiness checks.
func OptPostgres(user, password, database string) Opt {
	return func(o *opts) error {
		if err := OptEnv("POSTGRES_USER", user)(o); err != nil {
			return err
		}
		if err := OptEnv("POSTGRES_PASSWORD", password)(o); err != nil {
			return err
		}
		if err := OptEnv("POSTGRES_DB", database)(o); err != nil {
			return err
		}
		if err := OptPorts(pgxPort)(o); err != nil {
			return err
		}
		o.appendWaitStrategy(wait.ForSQL(nat.Port(pgxPort), "pgx", func(host string, port nat.Port) string {
			return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port.Port(), database)
		}))
		return nil
	}
}

// OptPostgresSetting adds a PostgreSQL configuration setting via -c flag
func OptPostgresSetting(key, value string) Opt {
	return func(o *opts) error {
		o.req.Cmd = append(o.req.Cmd, "-c", key+"="+value)
		return nil
	}
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// appendWaitStrategy merges a wait strategy into the current request.
func (o *opts) appendWaitStrategy(strategy wait.Strategy) {
	if o.req.WaitingFor == nil {
		o.req.WaitingFor = strategy
	} else if multi, ok := o.req.WaitingFor.(*wait.MultiStrategy); ok {
		multi.Strategies = append(multi.Strategies, strategy)
	} else {
		o.req.WaitingFor = wait.ForAll(o.req.WaitingFor, strategy)
	}
}
