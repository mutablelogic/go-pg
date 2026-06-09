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
// LIST OBJECTS TESTS

func Test_Manager_ListObjects(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("ListAll", func(t *testing.T) {
		objects, err := mgr.ListObjects(context.TODO(), schema.ObjectListRequest{})
		assert.NoError(err)
		assert.NotNil(objects)
		// Count should match body length (within limit)
		assert.LessOrEqual(len(objects.Body), int(objects.Count))
	})

	t.Run("ListWithPagination", func(t *testing.T) {
		limit := uint64(1)
		objects, err := mgr.ListObjects(context.TODO(), schema.ObjectListRequest{
			OffsetLimit: pg.OffsetLimit{Limit: &limit},
		})
		assert.NoError(err)
		assert.NotNil(objects)
		assert.LessOrEqual(len(objects.Body), 1)
	})

	t.Run("ListByDatabase", func(t *testing.T) {
		dbName := "postgres"
		objects, err := mgr.ListObjects(context.TODO(), schema.ObjectListRequest{
			Database: &dbName,
		})
		assert.NoError(err)
		assert.NotNil(objects)
		// All objects should be from postgres database
		for _, obj := range objects.Body {
			assert.Equal(dbName, obj.Database)
		}
	})

	t.Run("ListBySchema", func(t *testing.T) {
		schemaName := "public"
		objects, err := mgr.ListObjects(context.TODO(), schema.ObjectListRequest{
			Schema: &schemaName,
		})
		assert.NoError(err)
		assert.NotNil(objects)
		// All objects should be from public schema
		for _, obj := range objects.Body {
			assert.Equal(schemaName, obj.Schema)
		}
	})

	t.Run("ListWithMultipleFilters", func(t *testing.T) {
		dbName := "postgres"
		schemaName := "public"
		objects, err := mgr.ListObjects(context.TODO(), schema.ObjectListRequest{
			Database: &dbName,
			Schema:   &schemaName,
		})
		assert.NoError(err)
		assert.NotNil(objects)
		// All objects should match both filters
		for _, obj := range objects.Body {
			assert.Equal(dbName, obj.Database)
			assert.Equal(schemaName, obj.Schema)
		}
	})

	t.Run("ListWithOffset", func(t *testing.T) {
		// First get all objects
		allObjects, err := mgr.ListObjects(context.TODO(), schema.ObjectListRequest{})
		assert.NoError(err)

		if allObjects.Count < 2 {
			t.Skip("Not enough objects to test offset")
		}

		// Get with offset
		limit := uint64(10)
		offset := uint64(1)
		objects, err := mgr.ListObjects(context.TODO(), schema.ObjectListRequest{
			OffsetLimit: pg.OffsetLimit{
				Offset: offset,
				Limit:  &limit,
			},
		})
		assert.NoError(err)
		assert.NotNil(objects)
		// Count should be the same, but body should start from offset
		assert.Equal(allObjects.Count, objects.Count)
	})

	t.Run("ListFromNonExistentDatabase", func(t *testing.T) {
		dbName := "non_existing_database_xyz"
		objects, err := mgr.ListObjects(context.TODO(), schema.ObjectListRequest{
			Database: &dbName,
		})
		assert.NoError(err)
		assert.NotNil(objects)
		// Should return empty list for non-existent database
		assert.Equal(uint64(0), objects.Count)
		assert.Empty(objects.Body)
	})
}

////////////////////////////////////////////////////////////////////////////////
// GET OBJECT TESTS

func Test_Manager_GetObject(t *testing.T) {
	assert := assert.New(t)
	conn := conn.Begin(t)
	defer conn.Close()

	mgr, err := manager.New(context.TODO(), conn)
	if !assert.NoError(err) {
		t.FailNow()
	}

	t.Run("GetNonExistentObject", func(t *testing.T) {
		_, err := mgr.GetObject(context.TODO(), "postgres", "public", "non_existing_object_xyz")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("GetEmptyDatabase", func(t *testing.T) {
		_, err := mgr.GetObject(context.TODO(), "", "public", "test")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("GetEmptyNamespace", func(t *testing.T) {
		_, err := mgr.GetObject(context.TODO(), "postgres", "", "test")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("GetEmptyName", func(t *testing.T) {
		_, err := mgr.GetObject(context.TODO(), "postgres", "public", "")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrBadParameter)
	})

	t.Run("GetFromNonExistentSchema", func(t *testing.T) {
		_, err := mgr.GetObject(context.TODO(), "postgres", "non_existing_schema_xyz", "test")
		assert.Error(err)
		assert.ErrorIs(err, pg.ErrNotFound)
	})

	t.Run("GetExistingObject", func(t *testing.T) {
		// First list objects to find one that exists
		dbName := "postgres"
		schemaName := "public"
		limit := uint64(1)
		objects, err := mgr.ListObjects(context.TODO(), schema.ObjectListRequest{
			Database:    &dbName,
			Schema:      &schemaName,
			OffsetLimit: pg.OffsetLimit{Limit: &limit},
		})
		if !assert.NoError(err) {
			t.FailNow()
		}

		if len(objects.Body) == 0 {
			t.Skip("No objects found in postgres.public schema")
		}

		// Get the first object
		obj := objects.Body[0]
		result, err := mgr.GetObject(context.TODO(), obj.Database, obj.Schema, obj.Name)
		assert.NoError(err)
		assert.NotNil(result)
		assert.Equal(obj.Name, result.Name)
		assert.Equal(obj.Schema, result.Schema)
		assert.Equal(obj.Database, result.Database)
		assert.Equal(obj.Type, result.Type)
		assert.Equal(obj.Owner, result.Owner)
	})
}
