package pg_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	test "github.com/mutablelogic/go-pg/pkg/test"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

// Global connection variable
var conn test.Conn

// Start up a container and test the pool
func TestMain(m *testing.M) {
	test.Main(m, &conn)
}

func Test_Pool_001(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	// Ping the database
	assert.NoError(conn.Ping(context.Background()))
}

func Test_Pool_002(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	// Create a table
	err := conn.Exec(context.Background(), "CREATE TABLE test (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
	if !assert.NoError(err) {
		t.FailNow()
	}

	// Insert a row
	var test Test
	assert.NoError(conn.Insert(context.Background(), &test, test))
	assert.NotEqual(0, test.Id)

	// Update a row
	test.Name = "Hello, World"
	assert.NoError(conn.Update(context.Background(), &test, test, test))
	assert.NotEqual(0, test.Id)
	assert.Equal("Hello, World", test.Name)

	// Get a row
	assert.NoError(conn.Get(context.Background(), &test, test))
	assert.NotEqual(0, test.Id)
	assert.Equal("Hello, World", test.Name)

	// Delete a row
	assert.NoError(conn.Delete(context.Background(), &test, test))
	assert.NotEqual(0, test.Id)
	assert.Equal("Hello, World", test.Name)

	// Insert 20 rows
	for i := 0; i < 20; i++ {
		assert.NoError(conn.Insert(context.Background(), &test, test))
		assert.NotEqual(0, test.Id)
	}

	// List rows
	var list TestList
	assert.NoError(conn.List(context.Background(), &list, list))
	assert.Equal(uint64(20), list.Count)
	assert.Equal(20, len(list.Tests))

	// Drop the table
	assert.NoError(conn.Exec(context.Background(), "DROP TABLE test"))
}

func Test_Pool_003(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	// Transaction
	err := conn.Tx(context.Background(), func(conn pg.Conn) error {
		// Create a table
		err := conn.Exec(context.Background(), "CREATE TABLE test (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Insert a row
		var test Test
		assert.NoError(conn.Insert(context.Background(), &test, test))
		assert.NotEqual(0, test.Id)

		// Update a row
		test.Name = "Hello, World"
		assert.NoError(conn.Update(context.Background(), &test, test, test))
		assert.NotEqual(0, test.Id)
		assert.Equal("Hello, World", test.Name)

		// Get a row
		assert.NoError(conn.Get(context.Background(), &test, test))
		assert.NotEqual(0, test.Id)
		assert.Equal("Hello, World", test.Name)

		// Delete a row
		assert.NoError(conn.Delete(context.Background(), &test, test))
		assert.NotEqual(0, test.Id)
		assert.Equal("Hello, World", test.Name)

		// Insert 20 rows
		for i := 0; i < 20; i++ {
			assert.NoError(conn.Insert(context.Background(), &test, test))
			assert.NotEqual(0, test.Id)
		}

		// List rows
		var list TestList
		assert.NoError(conn.List(context.Background(), &list, list))
		assert.Equal(uint64(20), list.Count)
		assert.Equal(20, len(list.Tests))

		// Commit
		return nil
	})
	assert.NoError(err)
}

func Test_Pool_004(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	channel := fmt.Sprintf("test_pool_subscribe_%d", time.Now().UnixNano())
	notifyCh, err := conn.Subscribe(ctx, channel)
	require.NoError(err)
	require.NoError(conn.Exec(context.Background(), fmt.Sprintf("SELECT pg_notify('%s', 'hello')", channel)))

	select {
	case notify, ok := <-notifyCh:
		if !ok {
			t.Fatal("subscription channel closed before notification")
		}
		assert.Equal(channel, notify.Channel)
		assert.Equal([]byte("hello"), notify.Payload)
	case <-ctx.Done():
		t.Fatal("timeout waiting for notification")
	}

	err = conn.Tx(context.Background(), func(tx pg.Conn) error {
		_, err := tx.Subscribe(context.Background(), channel)
		return err
	})
	require.ErrorIs(err, pg.ErrNotAvailable)

	err = conn.Bulk(context.Background(), func(tx pg.Conn) error {
		_, err := tx.Subscribe(context.Background(), channel)
		return err
	})
	require.ErrorIs(err, pg.ErrNotAvailable)
}

func Test_Pool_005(t *testing.T) {
	require := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	verbose := slices.Contains(os.Args, "-test.v=true")
	container, pool, err := test.NewPgxContainer(ctx, t.Name(), verbose, nil)
	require.NoError(err)
	defer container.Close(context.Background())

	channel := fmt.Sprintf("test_pool_close_%d", time.Now().UnixNano())
	started := make(chan struct{})
	closingStarted := make(chan struct{})
	closed := make(chan struct{})
	notifyCh, err := pool.Subscribe(context.Background(), channel)
	require.NoError(err)

	require.NoError(pool.Exec(context.Background(), fmt.Sprintf("SELECT pg_notify('%s', 'hello')", channel)))

	select {
	case notify, ok := <-notifyCh:
		if !ok {
			t.Fatal("subscription channel closed before notification")
		}
		require.Equal(channel, notify.Channel)
		require.Equal([]byte("hello"), notify.Payload)
		close(started)
	case <-ctx.Done():
		t.Fatal("timeout waiting for callback")
	}

	go func() {
		close(closingStarted)
		pool.Close()
		close(closed)
	}()

	select {
	case <-closingStarted:
	case <-ctx.Done():
		t.Fatal("timeout waiting for pool close to start")
	}

	select {
	case _, ok := <-notifyCh:
		if ok {
			t.Fatal("subscription channel remained open after pool close")
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for subscription channel to close")
	}

	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("timeout waiting for pool close")
	}
}

////////////////////////////////////////////////////////////////////////////////

type Test struct {
	Id   int
	Name string
}

type TestList struct {
	Count uint64
	Tests []Test
}

func (t Test) String() string {
	data, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}

func (t *Test) Scan(row pg.Row) error {
	return row.Scan(&t.Id, &t.Name)
}

func (l *TestList) Scan(row pg.Row) error {
	var t Test
	if err := t.Scan(row); err != nil {
		return err
	}
	l.Tests = append(l.Tests, t)
	return nil
}

func (l *TestList) ScanCount(row pg.Row) error {
	return row.Scan(&l.Count)
}

func (t Test) Insert(bind *pg.Bind) (string, error) {
	bind.Set("name", t.Name)
	return "INSERT INTO test (name) VALUES (@name) RETURNING id, name", nil
}

func (t Test) Select(bind *pg.Bind, op pg.Op) (string, error) {
	bind.Set("id", t.Id)
	switch op {
	case pg.Update:
		return "UPDATE test SET ${patch} WHERE id=@id RETURNING id, name", nil
	case pg.Get:
		return "SELECT id, name FROM test WHERE id=@id", nil
	case pg.Delete:
		return "DELETE FROM test WHERE id=@id RETURNING id, name", nil
	default:
		return "", fmt.Errorf("Invalid operation %q", op)
	}
}

func (t TestList) Select(bind *pg.Bind, op pg.Op) (string, error) {
	switch op {
	case pg.List:
		return "SELECT id, name FROM test", nil
	default:
		return "", fmt.Errorf("Invalid operation %q", op)
	}
}

func (t Test) Update(bind *pg.Bind) error {
	bind.Set("patch", `name=`+bind.Set("name", t.Name))
	return nil
}
