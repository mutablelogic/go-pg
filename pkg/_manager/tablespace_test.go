package manager_test

import (
	"context"
	"testing"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	manager "github.com/mutablelogic/go-pg/pkg/manager"
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	assert "github.com/stretchr/testify/assert"
)

////////////////////////////////////////////////////////////////////////////////
// LIST TABLESPACES TESTS

func Test_Manager_ListTablespaces(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("ListAll", func(t *testing.T) {
		tablespaces, err := mgr.ListTablespaces(context.TODO(), schema.TablespaceListRequest{})
		assert.NoError(err)
		assert.NotNil(tablespaces)
		assert.Equal(len(tablespaces.Body), int(tablespaces.Count))
		// Should have at least the default pg_default and pg_global tablespaces
		assert.GreaterOrEqual(tablespaces.Count, uint64(2))
	})

	t.Run("ListWithPagination", func(t *testing.T) {
		limit := uint64(1)
		tablespaces, err := mgr.ListTablespaces(context.TODO(), schema.TablespaceListRequest{
			OffsetLimit: pg.OffsetLimit{Limit: &limit},
		})
		assert.NoError(err)
		assert.NotNil(tablespaces)
		assert.LessOrEqual(len(tablespaces.Body), 1)
	})

	t.Run("ListWithOffset", func(t *testing.T) {
		// First get all tablespaces
		allTablespaces, err := mgr.ListTablespaces(context.TODO(), schema.TablespaceListRequest{})
		assert.NoError(err)

		if allTablespaces.Count < 2 {
			t.Skip("Not enough tablespaces to test offset")
		}

		// Get with offset
		limit := uint64(10)
		tablespaces, err := mgr.ListTablespaces(context.TODO(), schema.TablespaceListRequest{
			OffsetLimit: pg.OffsetLimit{Offset: 1, Limit: &limit},
		})
		assert.NoError(err)
		assert.NotNil(tablespaces)
		assert.Less(len(tablespaces.Body), int(allTablespaces.Count))
	})
}

////////////////////////////////////////////////////////////////////////////////
// GET TABLESPACE TESTS

func Test_Manager_GetTablespace(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("GetExisting", func(t *testing.T) {
		// pg_default tablespace should always exist
		tablespace, err := mgr.GetTablespace(context.TODO(), "pg_default")
		assert.NoError(err)
		assert.NotNil(tablespace)
		assert.Equal("pg_default", tablespace.Name)
	})

	t.Run("GetPgGlobal", func(t *testing.T) {
		// pg_global tablespace should always exist
		tablespace, err := mgr.GetTablespace(context.TODO(), "pg_global")
		assert.NoError(err)
		assert.NotNil(tablespace)
		assert.Equal("pg_global", tablespace.Name)
	})

	t.Run("GetNonExistent", func(t *testing.T) {
		_, err := mgr.GetTablespace(context.TODO(), "non_existing_tablespace_xyz")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("GetEmptyName", func(t *testing.T) {
		_, err := mgr.GetTablespace(context.TODO(), "")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})
}

////////////////////////////////////////////////////////////////////////////////
// CREATE TABLESPACE TESTS
//
// Note: Creating tablespaces requires a filesystem location that the postgres
// user can write to. In a containerized test environment, this may not be
// available, so these tests verify error handling for invalid inputs.

func Test_Manager_CreateTablespace(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("CreateEmptyName", func(t *testing.T) {
		_, err := mgr.CreateTablespace(context.TODO(), schema.TablespaceMeta{
			Name: "",
		}, "/tmp/test_tablespace")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("CreateReservedPrefix", func(t *testing.T) {
		_, err := mgr.CreateTablespace(context.TODO(), schema.TablespaceMeta{
			Name: "pg_reserved_test",
		}, "/tmp/test_tablespace")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("CreateEmptyLocation", func(t *testing.T) {
		_, err := mgr.CreateTablespace(context.TODO(), schema.TablespaceMeta{
			Name: "test_tablespace",
		}, "")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("CreateRelativeLocation", func(t *testing.T) {
		_, err := mgr.CreateTablespace(context.TODO(), schema.TablespaceMeta{
			Name: "test_tablespace",
		}, "relative/path")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})
}

////////////////////////////////////////////////////////////////////////////////
// DELETE TABLESPACE TESTS

func Test_Manager_DeleteTablespace(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("DeleteNonExistent", func(t *testing.T) {
		_, err := mgr.DeleteTablespace(context.TODO(), "non_existing_ts_xyz")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("DeleteEmptyName", func(t *testing.T) {
		_, err := mgr.DeleteTablespace(context.TODO(), "")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("DeleteSystemTablespace", func(t *testing.T) {
		// Attempting to delete pg_default should fail
		_, err := mgr.DeleteTablespace(context.TODO(), "pg_default")
		assert.Error(err)
	})
}

////////////////////////////////////////////////////////////////////////////////
// UPDATE TABLESPACE TESTS

func Test_Manager_UpdateTablespace(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("UpdateEmptyName", func(t *testing.T) {
		_, err := mgr.UpdateTablespace(context.TODO(), "", schema.TablespaceMeta{
			Name: "newname",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("UpdateNonExistent", func(t *testing.T) {
		_, err := mgr.UpdateTablespace(context.TODO(), "non_existing_ts_xyz", schema.TablespaceMeta{
			Name: "newname",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("UpdateToReservedPrefix", func(t *testing.T) {
		// Get an existing tablespace first
		tablespaces, err := mgr.ListTablespaces(context.TODO(), schema.TablespaceListRequest{})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Find a non-system tablespace to test with
		var testTs *schema.Tablespace
		for _, ts := range tablespaces.Body {
			if ts.Name != "pg_default" && ts.Name != "pg_global" {
				testTs = &ts
				break
			}
		}

		if testTs == nil {
			t.Skip("No non-system tablespace available for testing")
		}

		// Try to rename to reserved prefix
		_, err = mgr.UpdateTablespace(context.TODO(), testTs.Name, schema.TablespaceMeta{
			Name: "pg_reserved",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})
}
