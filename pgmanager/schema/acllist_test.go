package schema_test

import (
	"encoding/json"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-pg/pgmanager/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_ACLList_UnmarshalJSON(t *testing.T) {
	// [] is no access, which isn't nil, unlike null, or none, which leave it
	var meta schema.DatabaseMeta
	assert.NoError(t, json.Unmarshal([]byte(`{"acl":[]}`), &meta))
	assert.NotNil(t, meta.Acl)
	assert.Empty(t, meta.Acl)

	meta = schema.DatabaseMeta{}
	assert.NoError(t, json.Unmarshal([]byte(`{"acl":null}`), &meta))
	assert.Nil(t, meta.Acl)

	meta = schema.DatabaseMeta{}
	assert.NoError(t, json.Unmarshal([]byte(`{}`), &meta))
	assert.Nil(t, meta.Acl)

	meta = schema.DatabaseMeta{}
	assert.NoError(t, json.Unmarshal([]byte(`{"acl":[{"role":"PUBLIC","priv":["CONNECT"]}]}`), &meta))
	if assert.Len(t, meta.Acl, 1) {
		assert.Equal(t, "PUBLIC", meta.Acl[0].Role)
		assert.Equal(t, []string{"CONNECT"}, meta.Acl[0].Priv)
	}
}
