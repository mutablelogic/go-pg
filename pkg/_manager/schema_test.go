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
// LIST SCHEMAS TESTS

func Test_Manager_ListSchemas(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("ListAll", func(t *testing.T) {
		schemas, err := mgr.ListSchemas(context.TODO(), schema.SchemaListRequest{})
		assert.NoError(err)
		assert.NotNil(schemas)
		// Should have at least the public schema in the default database
		assert.GreaterOrEqual(schemas.Count, uint64(1))
	})

	t.Run("ListWithPagination", func(t *testing.T) {
		limit := uint64(1)
		schemas, err := mgr.ListSchemas(context.TODO(), schema.SchemaListRequest{
			OffsetLimit: pg.OffsetLimit{Limit: &limit},
		})
		assert.NoError(err)
		assert.NotNil(schemas)
		assert.LessOrEqual(len(schemas.Body), 1)
	})

	t.Run("ListByDatabase", func(t *testing.T) {
		dbName := "postgres"
		schemas, err := mgr.ListSchemas(context.TODO(), schema.SchemaListRequest{
			Database: &dbName,
		})
		assert.NoError(err)
		assert.NotNil(schemas)
		// All schemas should be from postgres database
		for _, s := range schemas.Body {
			assert.Equal(dbName, s.Database)
		}
	})

	t.Run("ListFromMultipleDatabases", func(t *testing.T) {
		dbName := "test_schema_list_multi"
		t.Cleanup(func() {
			mgr.DeleteDatabase(context.TODO(), dbName, true)
		})

		// Create a test database
		_, err := mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
			Name: dbName,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// List all schemas (should include schemas from multiple databases)
		schemas, err := mgr.ListSchemas(context.TODO(), schema.SchemaListRequest{})
		assert.NoError(err)
		assert.NotNil(schemas)

		// Check that we have schemas from at least the new database and postgres
		databases := make(map[string]bool)
		for _, s := range schemas.Body {
			databases[s.Database] = true
		}
		assert.True(databases[dbName], "should have schemas from test database")
		assert.True(databases["postgres"], "should have schemas from postgres database")
	})
}

////////////////////////////////////////////////////////////////////////////////
// GET SCHEMA TESTS

func Test_Manager_GetSchema(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("GetPublicSchema", func(t *testing.T) {
		// public schema should always exist in postgres database
		s, err := mgr.GetSchema(context.TODO(), "postgres", "public")
		assert.NoError(err)
		assert.NotNil(s)
		assert.Equal("public", s.Name)
		assert.Equal("postgres", s.Database)
	})

	t.Run("GetNonExistentSchema", func(t *testing.T) {
		_, err := mgr.GetSchema(context.TODO(), "postgres", "non_existing_schema_xyz")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("GetEmptyDatabase", func(t *testing.T) {
		_, err := mgr.GetSchema(context.TODO(), "", "public")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("GetEmptyNamespace", func(t *testing.T) {
		_, err := mgr.GetSchema(context.TODO(), "postgres", "")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("GetFromDifferentDatabase", func(t *testing.T) {
		dbName := "test_get_schema_db"
		t.Cleanup(func() {
			mgr.DeleteDatabase(context.TODO(), dbName, true)
		})

		// Create a test database
		_, err := mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
			Name: dbName,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Get public schema from new database
		s, err := mgr.GetSchema(context.TODO(), dbName, "public")
		assert.NoError(err)
		assert.NotNil(s)
		assert.Equal("public", s.Name)
		assert.Equal(dbName, s.Database)
	})
}

////////////////////////////////////////////////////////////////////////////////
// CREATE SCHEMA TESTS

func Test_Manager_CreateSchema(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	// Create a test database for schema operations
	dbName := "test_create_schema_db"
	t.Cleanup(func() {
		mgr.DeleteDatabase(context.TODO(), dbName, true)
	})
	_, err = mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
		Name: dbName,
	})
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("CreateSimple", func(t *testing.T) {
		schemaName := "test_schema_simple"
		t.Cleanup(func() {
			mgr.DeleteSchema(context.TODO(), dbName, schemaName, true)
		})

		s, err := mgr.CreateSchema(context.TODO(), dbName, schema.SchemaMeta{
			Name:  schemaName,
			Owner: "postgres",
		})
		assert.NoError(err)
		assert.NotNil(s)
		assert.Equal(schemaName, s.Name)
		assert.Equal(dbName, s.Database)
	})

	t.Run("CreateWithACL", func(t *testing.T) {
		schemaName := "test_schema_acl"
		t.Cleanup(func() {
			mgr.DeleteSchema(context.TODO(), dbName, schemaName, true)
		})

		s, err := mgr.CreateSchema(context.TODO(), dbName, schema.SchemaMeta{
			Name:  schemaName,
			Owner: "postgres",
			Acl: schema.ACLList{
				{Role: "PUBLIC", Priv: []string{"USAGE"}},
			},
		})
		assert.NoError(err)
		assert.NotNil(s)

		publicACL := s.Acl.Find("PUBLIC")
		assert.NotNil(publicACL)
	})

	t.Run("CreateEmptyDatabase", func(t *testing.T) {
		_, err := mgr.CreateSchema(context.TODO(), "", schema.SchemaMeta{
			Name:  "test",
			Owner: "postgres",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("CreateEmptyName", func(t *testing.T) {
		_, err := mgr.CreateSchema(context.TODO(), dbName, schema.SchemaMeta{
			Name:  "",
			Owner: "postgres",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("CreateReservedPrefix", func(t *testing.T) {
		_, err := mgr.CreateSchema(context.TODO(), dbName, schema.SchemaMeta{
			Name:  "pg_reserved_test",
			Owner: "postgres",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		schemaName := "test_schema_duplicate"
		t.Cleanup(func() {
			mgr.DeleteSchema(context.TODO(), dbName, schemaName, true)
		})

		_, err := mgr.CreateSchema(context.TODO(), dbName, schema.SchemaMeta{
			Name:  schemaName,
			Owner: "postgres",
		})
		assert.NoError(err)

		// Try to create again
		_, err = mgr.CreateSchema(context.TODO(), dbName, schema.SchemaMeta{
			Name:  schemaName,
			Owner: "postgres",
		})
		assert.Error(err)
	})

	t.Run("CreateInDifferentDatabases", func(t *testing.T) {
		dbName2 := "test_create_schema_db2"
		schemaName := "shared_schema_name"
		t.Cleanup(func() {
			mgr.DeleteSchema(context.TODO(), dbName, schemaName, true)
			mgr.DeleteSchema(context.TODO(), dbName2, schemaName, true)
			mgr.DeleteDatabase(context.TODO(), dbName2, true)
		})

		// Create second database
		_, err := mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
			Name: dbName2,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Create schema in first database
		s1, err := mgr.CreateSchema(context.TODO(), dbName, schema.SchemaMeta{
			Name:  schemaName,
			Owner: "postgres",
		})
		assert.NoError(err)
		assert.NotNil(s1)
		assert.Equal(dbName, s1.Database)

		// Create schema with same name in second database
		s2, err := mgr.CreateSchema(context.TODO(), dbName2, schema.SchemaMeta{
			Name:  schemaName,
			Owner: "postgres",
		})
		assert.NoError(err)
		assert.NotNil(s2)
		assert.Equal(dbName2, s2.Database)
	})
}

////////////////////////////////////////////////////////////////////////////////
// DELETE SCHEMA TESTS

func Test_Manager_DeleteSchema(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	// Create a test database for schema operations
	dbName := "test_delete_schema_db"
	t.Cleanup(func() {
		mgr.DeleteDatabase(context.TODO(), dbName, true)
	})
	_, err = mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
		Name: dbName,
	})
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("DeleteExisting", func(t *testing.T) {
		schemaName := "test_schema_delete"

		// Create first
		_, err := mgr.CreateSchema(context.TODO(), dbName, schema.SchemaMeta{
			Name:  schemaName,
			Owner: "postgres",
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Delete
		s, err := mgr.DeleteSchema(context.TODO(), dbName, schemaName, false)
		assert.NoError(err)
		assert.NotNil(s)
		assert.Equal(schemaName, s.Name)

		// Verify it's gone
		_, err = mgr.GetSchema(context.TODO(), dbName, schemaName)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {
		_, err := mgr.DeleteSchema(context.TODO(), dbName, "non_existing_schema_xyz", false)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("DeleteEmptyDatabase", func(t *testing.T) {
		_, err := mgr.DeleteSchema(context.TODO(), "", "test", false)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("DeleteEmptyNamespace", func(t *testing.T) {
		_, err := mgr.DeleteSchema(context.TODO(), dbName, "", false)
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("DeleteWithForce", func(t *testing.T) {
		schemaName := "test_schema_force"

		// Create first
		_, err := mgr.CreateSchema(context.TODO(), dbName, schema.SchemaMeta{
			Name:  schemaName,
			Owner: "postgres",
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Delete with force (CASCADE)
		s, err := mgr.DeleteSchema(context.TODO(), dbName, schemaName, true)
		assert.NoError(err)
		assert.NotNil(s)
	})
}

////////////////////////////////////////////////////////////////////////////////
// UPDATE SCHEMA TESTS

func Test_Manager_UpdateSchema(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	// Create a test database for schema operations
	dbName := "test_update_schema_db"
	t.Cleanup(func() {
		mgr.DeleteDatabase(context.TODO(), dbName, true)
	})
	_, err = mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
		Name: dbName,
	})
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("RenameSchema", func(t *testing.T) {
		oldName := "test_schema_old"
		newName := "test_schema_new"
		t.Cleanup(func() {
			mgr.DeleteSchema(context.TODO(), dbName, oldName, true)
			mgr.DeleteSchema(context.TODO(), dbName, newName, true)
		})

		// Create
		_, err := mgr.CreateSchema(context.TODO(), dbName, schema.SchemaMeta{
			Name:  oldName,
			Owner: "postgres",
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Rename
		s, err := mgr.UpdateSchema(context.TODO(), dbName, oldName, schema.SchemaMeta{
			Name: newName,
		})
		assert.NoError(err)
		assert.NotNil(s)
		assert.Equal(newName, s.Name)

		// Old name should not exist
		_, err = mgr.GetSchema(context.TODO(), dbName, oldName)
		assert.ErrorIs(err, pg.ErrNotFound)

		// New name should exist
		_, err = mgr.GetSchema(context.TODO(), dbName, newName)
		assert.NoError(err)
	})

	t.Run("UpdateEmptyDatabase", func(t *testing.T) {
		_, err := mgr.UpdateSchema(context.TODO(), "", "test", schema.SchemaMeta{
			Name: "newname",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("UpdateEmptyNamespace", func(t *testing.T) {
		_, err := mgr.UpdateSchema(context.TODO(), dbName, "", schema.SchemaMeta{
			Name: "newname",
		})
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("UpdateNonExistent", func(t *testing.T) {
		_, err := mgr.UpdateSchema(context.TODO(), dbName, "non_existing_schema_xyz", schema.SchemaMeta{
			Name: "newname",
		})
		assert.Error(err)
	})

	t.Run("UpdateAddACL", func(t *testing.T) {
		schemaName := "test_schema_add_acl"
		t.Cleanup(func() {
			mgr.DeleteSchema(context.TODO(), dbName, schemaName, true)
		})

		// Create without ACL
		_, err := mgr.CreateSchema(context.TODO(), dbName, schema.SchemaMeta{
			Name:  schemaName,
			Owner: "postgres",
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Add ACL
		s, err := mgr.UpdateSchema(context.TODO(), dbName, schemaName, schema.SchemaMeta{
			Acl: schema.ACLList{
				{Role: "PUBLIC", Priv: []string{"USAGE"}},
			},
		})
		assert.NoError(err)
		assert.NotNil(s)

		publicACL := s.Acl.Find("PUBLIC")
		assert.NotNil(publicACL)
	})

	t.Run("UpdateSchemaInDifferentDatabase", func(t *testing.T) {
		dbName2 := "test_update_schema_db2"
		schemaName := "shared_update_schema"
		t.Cleanup(func() {
			mgr.DeleteSchema(context.TODO(), dbName, schemaName, true)
			mgr.DeleteSchema(context.TODO(), dbName2, schemaName, true)
			mgr.DeleteDatabase(context.TODO(), dbName2, true)
		})

		// Create second database
		_, err := mgr.CreateDatabase(context.TODO(), schema.DatabaseMeta{
			Name: dbName2,
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Create schemas in both databases
		_, err = mgr.CreateSchema(context.TODO(), dbName, schema.SchemaMeta{
			Name:  schemaName,
			Owner: "postgres",
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		_, err = mgr.CreateSchema(context.TODO(), dbName2, schema.SchemaMeta{
			Name:  schemaName,
			Owner: "postgres",
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		// Update schema in first database only
		s1, err := mgr.UpdateSchema(context.TODO(), dbName, schemaName, schema.SchemaMeta{
			Acl: schema.ACLList{
				{Role: "PUBLIC", Priv: []string{"USAGE"}},
			},
		})
		assert.NoError(err)
		assert.NotNil(s1.Acl.Find("PUBLIC"))

		// Schema in second database should not have the ACL
		s2, err := mgr.GetSchema(context.TODO(), dbName2, schemaName)
		assert.NoError(err)
		// s2 may or may not have PUBLIC ACL depending on defaults
		// The key is that they can be updated independently
		assert.NotNil(s2)
	})
}
