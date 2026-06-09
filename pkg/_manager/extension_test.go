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
// LIST EXTENSIONS TESTS

func Test_Manager_ListExtensions(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("ListAll", func(t *testing.T) {
		extensions, err := mgr.ListExtensions(context.TODO(), schema.ExtensionListRequest{})
		assert.NoError(err)
		assert.NotNil(extensions)
		// Should have at least some available extensions (plpgsql is always available)
		assert.GreaterOrEqual(extensions.Count, uint64(1))
	})

	t.Run("ListWithPagination", func(t *testing.T) {
		limit := uint64(1)
		extensions, err := mgr.ListExtensions(context.TODO(), schema.ExtensionListRequest{
			OffsetLimit: pg.OffsetLimit{Limit: &limit},
		})
		assert.NoError(err)
		assert.NotNil(extensions)
		assert.LessOrEqual(len(extensions.Body), 1)
	})

	t.Run("ListWithOffset", func(t *testing.T) {
		// First get all extensions
		allExtensions, err := mgr.ListExtensions(context.TODO(), schema.ExtensionListRequest{})
		assert.NoError(err)

		if allExtensions.Count < 2 {
			t.Skip("Not enough extensions to test offset")
		}

		// Get with offset
		limit := uint64(100)
		extensions, err := mgr.ListExtensions(context.TODO(), schema.ExtensionListRequest{
			OffsetLimit: pg.OffsetLimit{Offset: 1, Limit: &limit},
		})
		assert.NoError(err)
		assert.NotNil(extensions)
		assert.Less(len(extensions.Body), int(allExtensions.Count))
	})

	t.Run("ListInstalledOnly", func(t *testing.T) {
		installed := true
		extensions, err := mgr.ListExtensions(context.TODO(), schema.ExtensionListRequest{
			Installed: &installed,
		})
		assert.NoError(err)
		assert.NotNil(extensions)
		// All returned extensions should have InstalledVersion set
		for _, ext := range extensions.Body {
			assert.NotNil(ext.InstalledVersion, "Extension %s should have InstalledVersion", ext.Name)
		}
	})

	t.Run("ListNotInstalledOnly", func(t *testing.T) {
		installed := false
		extensions, err := mgr.ListExtensions(context.TODO(), schema.ExtensionListRequest{
			Installed: &installed,
		})
		assert.NoError(err)
		assert.NotNil(extensions)
		// All returned extensions should NOT have InstalledVersion set
		for _, ext := range extensions.Body {
			assert.Nil(ext.InstalledVersion, "Extension %s should not have InstalledVersion", ext.Name)
		}
	})

	t.Run("ListByDatabase", func(t *testing.T) {
		database := "postgres"
		extensions, err := mgr.ListExtensions(context.TODO(), schema.ExtensionListRequest{
			Database: &database,
		})
		assert.NoError(err)
		assert.NotNil(extensions)
		// All returned extensions should be from the postgres database
		for _, ext := range extensions.Body {
			assert.Equal("postgres", ext.Database)
		}
	})

	t.Run("ListByNonExistentDatabase", func(t *testing.T) {
		database := "non_existent_database_xyz"
		_, err := mgr.ListExtensions(context.TODO(), schema.ExtensionListRequest{
			Database: &database,
		})
		// Should return an error for non-existent database
		assert.Error(err)
	})

	t.Run("ExtensionHasDatabaseWhenSpecified", func(t *testing.T) {
		// When querying a specific database, extensions should have Database set
		database := "postgres"
		extensions, err := mgr.ListExtensions(context.TODO(), schema.ExtensionListRequest{
			Database: &database,
		})
		assert.NoError(err)
		assert.NotNil(extensions)
		for _, ext := range extensions.Body {
			assert.Equal(database, ext.Database, "Extension %s should have Database set", ext.Name)
		}
	})

	t.Run("ExtensionNoDatabaseWhenClusterWide", func(t *testing.T) {
		// When no database specified, extensions should NOT have Database set (cluster-wide)
		extensions, err := mgr.ListExtensions(context.TODO(), schema.ExtensionListRequest{})
		assert.NoError(err)
		assert.NotNil(extensions)
		for _, ext := range extensions.Body {
			assert.Empty(ext.Database, "Extension %s should not have Database set for cluster-wide query", ext.Name)
		}
	})
}

////////////////////////////////////////////////////////////////////////////////
// GET EXTENSION TESTS

func Test_Manager_GetExtension(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("GetExistingExtension", func(t *testing.T) {
		// plpgsql is always available
		ext, err := mgr.GetExtension(context.TODO(), "plpgsql")
		assert.NoError(err)
		assert.NotNil(ext)
		assert.Equal("plpgsql", ext.Name)
		// Database field is not set for cluster-wide queries
		assert.Empty(ext.Database)
	})

	t.Run("GetNonExistentExtension", func(t *testing.T) {
		_, err := mgr.GetExtension(context.TODO(), "nonexistent_extension_xyz")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("GetEmptyName", func(t *testing.T) {
		_, err := mgr.GetExtension(context.TODO(), "")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("GetWhitespaceName", func(t *testing.T) {
		_, err := mgr.GetExtension(context.TODO(), "   ")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})
}

////////////////////////////////////////////////////////////////////////////////
// CREATE EXTENSION TESTS

func Test_Manager_CreateExtension(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("MissingDatabase", func(t *testing.T) {
		_, err := mgr.CreateExtension(context.TODO(), schema.ExtensionMeta{
			Name: "hstore",
		}, false)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("MissingName", func(t *testing.T) {
		_, err := mgr.CreateExtension(context.TODO(), schema.ExtensionMeta{
			Database: "postgres",
		}, false)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("CreateDblink", func(t *testing.T) {
		// dblink should be available since we use it for cross-database queries
		ext, err := mgr.CreateExtension(context.TODO(), schema.ExtensionMeta{
			Name:     "dblink",
			Database: "postgres",
		}, false)
		if !assert.NoError(err) {
			t.FailNow()
		}
		assert.Equal("dblink", ext.Name)
		assert.NotNil(ext.InstalledVersion)
	})

	t.Run("CreateWithCascade", func(t *testing.T) {
		// file_fdw is a simpler extension that should be available
		ext, err := mgr.CreateExtension(context.TODO(), schema.ExtensionMeta{
			Name:     "file_fdw",
			Database: "postgres",
		}, true)
		if !assert.NoError(err) {
			t.FailNow()
		}
		assert.Equal("file_fdw", ext.Name)
		assert.NotNil(ext.InstalledVersion)
	})
}

////////////////////////////////////////////////////////////////////////////////
// UPDATE EXTENSION TESTS

func Test_Manager_UpdateExtension(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("MissingDatabase", func(t *testing.T) {
		_, err := mgr.UpdateExtension(context.TODO(), "plpgsql", schema.ExtensionMeta{
			Version: "1.0",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("MissingName", func(t *testing.T) {
		_, err := mgr.UpdateExtension(context.TODO(), "", schema.ExtensionMeta{
			Database: "postgres",
			Version:  "1.0",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("NameCannotBeChanged", func(t *testing.T) {
		_, err := mgr.UpdateExtension(context.TODO(), "plpgsql", schema.ExtensionMeta{
			Database: "postgres",
			Name:     "newname",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
		assert.Contains(err.Error(), "name cannot be changed")
	})

	t.Run("OwnerCannotBeChanged", func(t *testing.T) {
		_, err := mgr.UpdateExtension(context.TODO(), "plpgsql", schema.ExtensionMeta{
			Database: "postgres",
			Owner:    "newowner",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
		assert.Contains(err.Error(), "owner cannot be changed")
	})

	t.Run("UpdateToLatest", func(t *testing.T) {
		// Update plpgsql (always installed) to latest version
		ext, err := mgr.UpdateExtension(context.TODO(), "plpgsql", schema.ExtensionMeta{
			Database: "postgres",
		})
		if !assert.NoError(err) {
			t.FailNow()
		}
		assert.Equal("plpgsql", ext.Name)
		assert.Equal("postgres", ext.Database)
	})
}

////////////////////////////////////////////////////////////////////////////////
// DELETE EXTENSION TESTS

func Test_Manager_DeleteExtension(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("MissingDatabase", func(t *testing.T) {
		err := mgr.DeleteExtension(context.TODO(), "", "dblink", false)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("MissingName", func(t *testing.T) {
		err := mgr.DeleteExtension(context.TODO(), "postgres", "", false)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {
		// Deleting a non-existent extension should fail
		err := mgr.DeleteExtension(context.TODO(), "postgres", "nonexistent_ext_xyz", false)
		assert.Error(err)
	})

	t.Run("DeleteAndRecreate", func(t *testing.T) {
		// First create an extension
		ext, err := mgr.CreateExtension(context.TODO(), schema.ExtensionMeta{
			Name:     "file_fdw",
			Database: "postgres",
		}, false)
		if !assert.NoError(err) {
			t.FailNow()
		}
		assert.Equal("file_fdw", ext.Name)
		assert.NotNil(ext.InstalledVersion)

		// Delete it
		err = mgr.DeleteExtension(context.TODO(), "postgres", "file_fdw", false)
		assert.NoError(err)

		// Verify it's not installed anymore
		ext2, err := mgr.GetExtension(context.TODO(), "file_fdw")
		if !assert.NoError(err) {
			t.FailNow()
		}
		assert.Nil(ext2.InstalledVersion)
	})

	t.Run("DeleteWithCascade", func(t *testing.T) {
		// Create an extension first
		_, err := mgr.CreateExtension(context.TODO(), schema.ExtensionMeta{
			Name:     "file_fdw",
			Database: "postgres",
		}, false)
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Delete with cascade
		err = mgr.DeleteExtension(context.TODO(), "postgres", "file_fdw", true)
		assert.NoError(err)
	})
}
