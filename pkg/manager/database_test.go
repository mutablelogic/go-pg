package manager_test

import (
	"context"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pkg/manager"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	test "github.com/mutablelogic/go-pg/pkg/test"
	assert "github.com/stretchr/testify/assert"
)

// Global connection variable
var conn test.Conn

// Start up a container and test the pool
func TestMain(m *testing.M) {
	test.Main(m, func(pool pg.PoolConn) (func(), error) {
		conn = test.Conn{PoolConn: pool}
		return nil, nil
	})
}

////////////////////////////////////////////////////////////////////////////////
// MANAGER LIFECYCLE TESTS

func Test_Manager_New(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	t.Run("ValidConnection", func(t *testing.T) {
		mgr, err := manager.New(context.TODO(), conn)
		assert.NoError(err)
		assert.NotNil(mgr)
	})

	t.Run("NilConnection", func(t *testing.T) {
		_, err := manager.New(context.TODO(), nil)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})
}

////////////////////////////////////////////////////////////////////////////////
// LIST DATABASES TESTS

func Test_Manager_ListDatabases(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("ListAll", func(t *testing.T) {
		databases, err := mgr.ListDatabases(context.TODO(), schema.DatabaseListRequest{})
		assert.NoError(err)
		assert.NotNil(databases)
		assert.Equal(len(databases.Body), int(databases.Count))
		// Should have at least the default postgres database
		assert.GreaterOrEqual(databases.Count, uint64(1))
	})

	t.Run("ListWithPagination", func(t *testing.T) {
		limit := uint64(1)
		databases, err := mgr.ListDatabases(context.TODO(), schema.DatabaseListRequest{
			OffsetLimit: pg.OffsetLimit{Limit: &limit},
		})
		assert.NoError(err)
		assert.NotNil(databases)
		assert.LessOrEqual(len(databases.Body), 1)
	})
}

////////////////////////////////////////////////////////////////////////////////
// GET DATABASE TESTS

func Test_Manager_GetDatabase(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("GetExisting", func(t *testing.T) {
		// postgres database should always exist
		database, err := mgr.GetDatabase(context.TODO(), "postgres")
		assert.NoError(err)
		assert.NotNil(database)
		assert.Equal("postgres", database.Name)
	})

	t.Run("GetNonExistent", func(t *testing.T) {
		_, err := mgr.GetDatabase(context.TODO(), "non_existing_database_xyz")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("GetEmptyName", func(t *testing.T) {
		_, err := mgr.GetDatabase(context.TODO(), "")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})
}

////////////////////////////////////////////////////////////////////////////////
// CREATE DATABASE TESTS

func Test_Manager_CreateDatabase(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("CreateSimple", func(t *testing.T) {
		dbName := "test_create_simple"
		t.Cleanup(func() {
			mgr.DeleteDatabase(context.TODO(), dbName, true)
		})

		database, err := mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
			Name: dbName,
		})
		assert.NoError(err)
		assert.NotNil(database)
		assert.Equal(dbName, database.Name)
		assert.NotEmpty(database.Owner)
	})

	t.Run("CreateWithACL", func(t *testing.T) {
		dbName := "test_create_with_acl"
		t.Cleanup(func() {
			mgr.DeleteDatabase(context.TODO(), dbName, true)
		})

		database, err := mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
			Name: dbName,
			Acl: schema.ACLList{
				{Role: "PUBLIC", Priv: []string{"CONNECT"}},
			},
		})
		assert.NoError(err)
		assert.NotNil(database)

		publicACL := database.Acl.Find("PUBLIC")
		assert.NotNil(publicACL)
	})

	t.Run("CreateEmptyName", func(t *testing.T) {
		_, err := mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
			Name: "",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("CreateReservedPrefix", func(t *testing.T) {
		_, err := mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
			Name: "pg_reserved_test",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		dbName := "test_create_duplicate"
		t.Cleanup(func() {
			mgr.DeleteDatabase(context.TODO(), dbName, true)
		})

		_, err := mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
			Name: dbName,
		})
		assert.NoError(err)

		// Try to create again
		_, err = mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
			Name: dbName,
		})
		assert.Error(err)
	})
}

////////////////////////////////////////////////////////////////////////////////
// DELETE DATABASE TESTS

func Test_Manager_DeleteDatabase(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("DeleteExisting", func(t *testing.T) {
		dbName := "test_delete_existing"

		// Create first
		_, err := mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
			Name: dbName,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Delete
		database, err := mgr.DeleteDatabase(context.TODO(), dbName, false)
		assert.NoError(err)
		assert.NotNil(database)
		assert.Equal(dbName, database.Name)

		// Verify it's gone
		_, err = mgr.GetDatabase(context.TODO(), dbName)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {
		_, err := mgr.DeleteDatabase(context.TODO(), "non_existing_db_xyz", false)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("DeleteEmptyName", func(t *testing.T) {
		_, err := mgr.DeleteDatabase(context.TODO(), "", false)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("DeleteWithForce", func(t *testing.T) {
		dbName := "test_delete_force"

		// Create first
		_, err := mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
			Name: dbName,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Delete with force
		database, err := mgr.DeleteDatabase(context.TODO(), dbName, true)
		assert.NoError(err)
		assert.NotNil(database)
	})
}

////////////////////////////////////////////////////////////////////////////////
// UPDATE DATABASE TESTS

func Test_Manager_UpdateDatabase(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("RenameDatabase", func(t *testing.T) {
		oldName := "test_rename_old"
		newName := "test_rename_new"
		t.Cleanup(func() {
			mgr.DeleteDatabase(context.TODO(), oldName, true)
			mgr.DeleteDatabase(context.TODO(), newName, true)
		})

		// Create
		_, err := mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
			Name: oldName,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Rename
		database, err := mgr.UpdateDatabase(context.TODO(), oldName, schema.DatabaseMeta{
			Name: newName,
		})
		assert.NoError(err)
		assert.NotNil(database)
		assert.Equal(newName, database.Name)

		// Old name should not exist
		_, err = mgr.GetDatabase(context.TODO(), oldName)
		assert.ErrorIs(err, pg.ErrNotFound)

		// New name should exist
		_, err = mgr.GetDatabase(context.TODO(), newName)
		assert.NoError(err)
	})

	t.Run("UpdateEmptyName", func(t *testing.T) {
		_, err := mgr.UpdateDatabase(context.TODO(), "", schema.DatabaseMeta{
			Name: "newname",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("UpdateToReservedPrefix", func(t *testing.T) {
		dbName := "test_update_reserved"
		t.Cleanup(func() {
			mgr.DeleteDatabase(context.TODO(), dbName, true)
		})

		// Create
		_, err := mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
			Name: dbName,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Try to rename to reserved prefix
		_, err = mgr.UpdateDatabase(context.TODO(), dbName, schema.DatabaseMeta{
			Name: "pg_reserved",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("UpdateNonExistent", func(t *testing.T) {
		_, err := mgr.UpdateDatabase(context.TODO(), "non_existing_db_xyz", schema.DatabaseMeta{
			Name: "newname",
		})
		assert.Error(err)
	})

	t.Run("UpdateAddACL", func(t *testing.T) {
		dbName := "test_update_add_acl"
		t.Cleanup(func() {
			mgr.DeleteDatabase(context.TODO(), dbName, true)
		})

		// Create without ACL
		_, err := mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
			Name: dbName,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Add ACL
		database, err := mgr.UpdateDatabase(context.TODO(), dbName, schema.DatabaseMeta{
			Acl: schema.ACLList{
				{Role: "PUBLIC", Priv: []string{"CONNECT"}},
			},
		})
		assert.NoError(err)
		assert.NotNil(database)

		publicACL := database.Acl.Find("PUBLIC")
		assert.NotNil(publicACL)
	})
}
