package schema

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Postmaster is the server's process, as the server says it is: its data
// directory, in which its postmaster.pid is, and when it started
type Postmaster struct {
	DataDirectory string
	Started       time.Time
}

// PostmasterPID is a postmaster.pid: the postmaster's process, its data
// directory, and when it started, to the second
type PostmasterPID struct {
	Pid           int
	DataDirectory string
	Started       time.Time
}

// Restart is the server, once it's restarted: when it started
type Restart struct {
	Started time.Time `json:"started"`
}

// PostmasterPIDFile is the name of the postmaster's file, in its data
// directory
const PostmasterPIDFile = "postmaster.pid"

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (r Restart) String() string {
	return types.Stringify(r)
}

////////////////////////////////////////////////////////////////////////////////
// SELECT

func (Postmaster) Select(bind *pg.Bind, op pg.Op) (string, error) {
	switch op {
	case pg.Get:
		return `SELECT current_setting('data_directory'), pg_postmaster_start_time()`, nil
	default:
		return "", pg.ErrNotImplemented.Withf("unsupported Postmaster operation %q", op)
	}
}

func (p *Postmaster) Scan(row pg.Row) error {
	return row.Scan(&p.DataDirectory, &p.Started)
}

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// ReadPostmasterPID reads a postmaster.pid, whose first lines are the
// postmaster's process, its data directory, and when it started, in seconds
// since the epoch
func ReadPostmasterPID(r io.Reader) (*PostmasterPID, error) {
	var lines []string
	scanner := bufio.NewScanner(r)
	for scanner.Scan() && len(lines) < 3 {
		lines = append(lines, strings.TrimSpace(scanner.Text()))
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	} else if len(lines) < 3 {
		return nil, fmt.Errorf("%s has %d lines, not the process, data directory and start time", PostmasterPIDFile, len(lines))
	}
	pid, err := strconv.Atoi(lines[0])
	if err != nil || pid <= 0 {
		return nil, fmt.Errorf("%s has no process: %q", PostmasterPIDFile, lines[0])
	}
	started, err := strconv.ParseInt(lines[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s has no start time: %q", PostmasterPIDFile, lines[2])
	}
	return &PostmasterPID{Pid: pid, DataDirectory: lines[1], Started: time.Unix(started, 0)}, nil
}

// Matches returns an error unless the postmaster.pid is the server's: its
// data directory is the same, and it started at the same time, which the
// file has to the second, and the server a little later
func (p PostmasterPID) Matches(server Postmaster) error {
	if p.DataDirectory != server.DataDirectory {
		return fmt.Errorf("%s is of %q, not the server's data directory %q", PostmasterPIDFile, p.DataDirectory, server.DataDirectory)
	}
	if diff := server.Started.Sub(p.Started); diff < -time.Second || diff > 5*time.Second {
		return fmt.Errorf("%s started at %v, not when the server did, at %v", PostmasterPIDFile, p.Started, server.Started)
	}
	return nil
}
