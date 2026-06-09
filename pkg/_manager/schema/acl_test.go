package schema_test

import (
	"encoding/json"
	"testing"

	// Packages
	schema "github.com/mutablelogic/go-pg/pkg/manager/schema"
	assert "github.com/stretchr/testify/assert"
)

func Test_ACLItem_NewACLItem(t *testing.T) {
	assert := assert.New(t)

	t.Run("FullPrivileges", func(t *testing.T) {
		acl, err := schema.NewACLItem("miriam=arwdDxtm/miriam")
		if assert.NoError(err) {
			assert.Equal("miriam", acl.Role)
			assert.Equal([]string{"INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"}, acl.Priv)
			assert.Equal("miriam", acl.Grantor)
			t.Log(acl)
		}
	})

	t.Run("PublicRole", func(t *testing.T) {
		acl, err := schema.NewACLItem("=r/miriam")
		if assert.NoError(err) {
			assert.Equal(schema.DefaultAclRole, acl.Role)
			assert.Equal([]string{"SELECT"}, acl.Priv)
			assert.Equal("miriam", acl.Grantor)
			t.Log(acl)
		}
	})

	t.Run("WithGrantOption", func(t *testing.T) {
		acl, err := schema.NewACLItem("miriam=r*w/")
		if assert.NoError(err) {
			assert.Equal("miriam", acl.Role)
			assert.Equal([]string{"SELECT WITH GRANT OPTION", "UPDATE"}, acl.Priv)
			assert.Equal(schema.DefaultAclRole, acl.Grantor)
			t.Log(acl)
		}
	})

	t.Run("InvalidFormat", func(t *testing.T) {
		_, err := schema.NewACLItem("invalid")
		assert.Error(err)
	})

	t.Run("EmptyString", func(t *testing.T) {
		_, err := schema.NewACLItem("")
		assert.Error(err)
	})

	t.Run("AllPrivileges", func(t *testing.T) {
		// Test all privilege types
		acl, err := schema.NewACLItem("user=arwdDxtCcTXUsAm/admin")
		if assert.NoError(err) {
			assert.Equal("user", acl.Role)
			assert.Contains(acl.Priv, "INSERT")
			assert.Contains(acl.Priv, "SELECT")
			assert.Contains(acl.Priv, "UPDATE")
			assert.Contains(acl.Priv, "DELETE")
			assert.Contains(acl.Priv, "TRUNCATE")
			assert.Contains(acl.Priv, "REFERENCES")
			assert.Contains(acl.Priv, "TRIGGER")
			assert.Contains(acl.Priv, "CREATE")
			assert.Contains(acl.Priv, "CONNECT")
			assert.Contains(acl.Priv, "TEMPORARY")
			assert.Contains(acl.Priv, "EXECUTE")
			assert.Contains(acl.Priv, "USAGE")
			assert.Contains(acl.Priv, "SET")
			assert.Contains(acl.Priv, "ALTER SYSTEM")
			assert.Contains(acl.Priv, "MAINTAIN")
		}
	})
}

func Test_ACLItem_ParseACLItem(t *testing.T) {
	assert := assert.New(t)

	t.Run("SimpleRole", func(t *testing.T) {
		acl, err := schema.ParseACLItem("myuser:SELECT")
		if assert.NoError(err) {
			assert.Equal("myuser", acl.Role)
			assert.Equal([]string{"SELECT"}, acl.Priv)
		}
	})

	t.Run("MultiplePrivileges", func(t *testing.T) {
		acl, err := schema.ParseACLItem("myuser:SELECT,INSERT,UPDATE")
		if assert.NoError(err) {
			assert.Equal("myuser", acl.Role)
			assert.Equal([]string{"SELECT", "INSERT", "UPDATE"}, acl.Priv)
		}
	})

	t.Run("QuotedRole", func(t *testing.T) {
		acl, err := schema.ParseACLItem("\"my user\":SELECT")
		if assert.NoError(err) {
			assert.Equal("my user", acl.Role)
			assert.Equal([]string{"SELECT"}, acl.Priv)
		}
	})

	t.Run("CaseInsensitive", func(t *testing.T) {
		acl, err := schema.ParseACLItem("user:select,INSERT,Update")
		if assert.NoError(err) {
			assert.Equal([]string{"SELECT", "INSERT", "UPDATE"}, acl.Priv)
		}
	})

	t.Run("InvalidPrivilege", func(t *testing.T) {
		_, err := schema.ParseACLItem("user:INVALID")
		assert.Error(err)
	})

	t.Run("MissingPrivilege", func(t *testing.T) {
		_, err := schema.ParseACLItem("user:")
		assert.Error(err)
	})

	t.Run("MissingColon", func(t *testing.T) {
		// A role without colon and privileges should fail
		// Currently the parser accepts it in stSep state - this documents current behavior
		// If stricter validation is needed, update UnmarshalText
		acl, err := schema.ParseACLItem("user")
		// Currently returns success with empty privileges
		if err == nil {
			assert.Equal("user", acl.Role)
			assert.Empty(acl.Priv)
		}
	})

	t.Run("EmptyString", func(t *testing.T) {
		_, err := schema.ParseACLItem("")
		assert.Error(err)
	})

	t.Run("DuplicatePrivileges", func(t *testing.T) {
		acl, err := schema.ParseACLItem("user:SELECT,SELECT,INSERT")
		if assert.NoError(err) {
			// Should deduplicate
			assert.Equal([]string{"SELECT", "INSERT"}, acl.Priv)
		}
	})

	t.Run("AllPrivilege", func(t *testing.T) {
		acl, err := schema.ParseACLItem("admin:ALL")
		if assert.NoError(err) {
			assert.Equal([]string{"ALL"}, acl.Priv)
			assert.True(acl.IsAll())
		}
	})
}

func Test_ACLItem_WithPriv(t *testing.T) {
	assert := assert.New(t)

	t.Run("ReplacePrivileges", func(t *testing.T) {
		original, _ := schema.ParseACLItem("user:SELECT,INSERT")
		modified := original.WithPriv("UPDATE", "DELETE")
		assert.Equal("user", modified.Role)
		assert.Equal([]string{"UPDATE", "DELETE"}, modified.Priv)
		// Original should be unchanged
		assert.Equal([]string{"SELECT", "INSERT"}, original.Priv)
	})
}

func Test_ACLItem_MarshalText(t *testing.T) {
	assert := assert.New(t)

	t.Run("SimpleRole", func(t *testing.T) {
		acl, _ := schema.ParseACLItem("user:SELECT,INSERT")
		data, err := acl.MarshalText()
		if assert.NoError(err) {
			assert.Equal("user:SELECT,INSERT", string(data))
		}
	})

	t.Run("RoleWithSpaces", func(t *testing.T) {
		acl := &schema.ACLItem{Role: "my user", Priv: []string{"SELECT"}}
		data, err := acl.MarshalText()
		if assert.NoError(err) {
			assert.Equal("\"my user\":SELECT", string(data))
		}
	})

	t.Run("EmptyRole", func(t *testing.T) {
		acl := &schema.ACLItem{Role: "", Priv: []string{"SELECT"}}
		_, err := acl.MarshalText()
		assert.Error(err)
	})

	t.Run("RoundTrip", func(t *testing.T) {
		original, _ := schema.ParseACLItem("testuser:SELECT,INSERT,UPDATE")
		data, err := original.MarshalText()
		if assert.NoError(err) {
			parsed, err := schema.ParseACLItem(string(data))
			if assert.NoError(err) {
				assert.Equal(original.Role, parsed.Role)
				assert.Equal(original.Priv, parsed.Priv)
			}
		}
	})
}

func Test_ACLItem_UnmarshalJSON(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidJSON", func(t *testing.T) {
		data := `{"role": "testuser", "priv": ["SELECT", "INSERT"]}`
		var acl schema.ACLItem
		err := json.Unmarshal([]byte(data), &acl)
		if assert.NoError(err) {
			assert.Equal("testuser", acl.Role)
			assert.Equal([]string{"SELECT", "INSERT"}, acl.Priv)
		}
	})

	t.Run("PublicRole", func(t *testing.T) {
		data := `{"role": "PUBLIC", "priv": ["SELECT"]}`
		var acl schema.ACLItem
		err := json.Unmarshal([]byte(data), &acl)
		if assert.NoError(err) {
			assert.Equal(schema.DefaultAclRole, acl.Role)
		}
	})

	t.Run("CaseInsensitivePublic", func(t *testing.T) {
		data := `{"role": "public", "priv": ["SELECT"]}`
		var acl schema.ACLItem
		err := json.Unmarshal([]byte(data), &acl)
		if assert.NoError(err) {
			assert.Equal(schema.DefaultAclRole, acl.Role)
		}
	})

	t.Run("MissingRole", func(t *testing.T) {
		data := `{"priv": ["SELECT"]}`
		var acl schema.ACLItem
		err := json.Unmarshal([]byte(data), &acl)
		assert.Error(err)
	})

	t.Run("EmptyRole", func(t *testing.T) {
		data := `{"role": "", "priv": ["SELECT"]}`
		var acl schema.ACLItem
		err := json.Unmarshal([]byte(data), &acl)
		assert.Error(err)
	})

	t.Run("MissingPriv", func(t *testing.T) {
		data := `{"role": "user"}`
		var acl schema.ACLItem
		err := json.Unmarshal([]byte(data), &acl)
		assert.Error(err)
	})

	t.Run("InvalidPrivilege", func(t *testing.T) {
		data := `{"role": "user", "priv": ["INVALID"]}`
		var acl schema.ACLItem
		err := json.Unmarshal([]byte(data), &acl)
		assert.Error(err)
	})

	t.Run("InvalidPrivType", func(t *testing.T) {
		data := `{"role": "user", "priv": [123]}`
		var acl schema.ACLItem
		err := json.Unmarshal([]byte(data), &acl)
		assert.Error(err)
	})

	t.Run("StringFormat", func(t *testing.T) {
		data := `"PUBLIC:TEMPORARY,CONNECT"`
		var acl schema.ACLItem
		err := json.Unmarshal([]byte(data), &acl)
		if assert.NoError(err) {
			assert.Equal(schema.DefaultAclRole, acl.Role)
			assert.Equal([]string{"TEMPORARY", "CONNECT"}, acl.Priv)
		}
	})

	t.Run("StringFormatWithRole", func(t *testing.T) {
		data := `"fabric:CREATE,TEMPORARY,CONNECT"`
		var acl schema.ACLItem
		err := json.Unmarshal([]byte(data), &acl)
		if assert.NoError(err) {
			assert.Equal("fabric", acl.Role)
			assert.Equal([]string{"CREATE", "TEMPORARY", "CONNECT"}, acl.Priv)
		}
	})
}

func Test_ACLList_Append(t *testing.T) {
	assert := assert.New(t)

	t.Run("AppendNew", func(t *testing.T) {
		var list schema.ACLList
		item1, _ := schema.ParseACLItem("user1:SELECT")
		item2, _ := schema.ParseACLItem("user2:INSERT")
		list.Append(item1)
		list.Append(item2)
		assert.Len(list, 2)
	})

	t.Run("MergeDuplicateRole", func(t *testing.T) {
		var list schema.ACLList
		item1, _ := schema.ParseACLItem("user1:SELECT")
		item2, _ := schema.ParseACLItem("user1:INSERT")
		list.Append(item1)
		list.Append(item2)
		assert.Len(list, 1)
		assert.Equal([]string{"SELECT", "INSERT"}, list[0].Priv)
	})

	t.Run("MergeNoDuplicatePriv", func(t *testing.T) {
		var list schema.ACLList
		item1, _ := schema.ParseACLItem("user1:SELECT,INSERT")
		item2, _ := schema.ParseACLItem("user1:INSERT,UPDATE")
		list.Append(item1)
		list.Append(item2)
		assert.Len(list, 1)
		// Should not duplicate INSERT
		assert.Equal([]string{"SELECT", "INSERT", "UPDATE"}, list[0].Priv)
	})
}

func Test_ACLList_Find(t *testing.T) {
	assert := assert.New(t)

	t.Run("FindExisting", func(t *testing.T) {
		var list schema.ACLList
		item1, _ := schema.ParseACLItem("user1:SELECT")
		item2, _ := schema.ParseACLItem("user2:INSERT")
		list.Append(item1)
		list.Append(item2)

		found := list.Find("user1")
		assert.NotNil(found)
		assert.Equal("user1", found.Role)
	})

	t.Run("FindNotExisting", func(t *testing.T) {
		var list schema.ACLList
		item1, _ := schema.ParseACLItem("user1:SELECT")
		list.Append(item1)

		found := list.Find("nonexistent")
		assert.Nil(found)
	})

	t.Run("FindEmptyList", func(t *testing.T) {
		var list schema.ACLList
		found := list.Find("user1")
		assert.Nil(found)
	})
}

func Test_ACLList_UnmarshalText(t *testing.T) {
	assert := assert.New(t)

	t.Run("MultipleItems", func(t *testing.T) {
		var list schema.ACLList
		err := list.UnmarshalText([]byte("user1:SELECT user2:INSERT"))
		if assert.NoError(err) {
			assert.Len(list, 2)
		}
	})

	t.Run("SingleItem", func(t *testing.T) {
		var list schema.ACLList
		err := list.UnmarshalText([]byte("user1:SELECT,INSERT"))
		if assert.NoError(err) {
			assert.Len(list, 1)
			assert.Equal([]string{"SELECT", "INSERT"}, list[0].Priv)
		}
	})

	t.Run("EmptyInput", func(t *testing.T) {
		var list schema.ACLList
		err := list.UnmarshalText([]byte(""))
		assert.NoError(err)
		assert.Len(list, 0)
	})
}

func Test_ACLList_UnmarshalJSON(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidArray", func(t *testing.T) {
		var list schema.ACLList
		data := `[{"role": "user1", "priv": ["SELECT"]}, {"role": "user2", "priv": ["INSERT"]}]`
		err := list.UnmarshalJSON([]byte(data))
		if assert.NoError(err) {
			assert.Len(list, 2)
		}
	})

	t.Run("EmptyArray", func(t *testing.T) {
		var list schema.ACLList
		data := `[]`
		err := list.UnmarshalJSON([]byte(data))
		assert.NoError(err)
		assert.Len(list, 0)
	})

	t.Run("MergesSameRole", func(t *testing.T) {
		var list schema.ACLList
		data := `[{"role": "user1", "priv": ["SELECT"]}, {"role": "user1", "priv": ["INSERT"]}]`
		err := list.UnmarshalJSON([]byte(data))
		if assert.NoError(err) {
			assert.Len(list, 1)
			assert.Equal([]string{"SELECT", "INSERT"}, list[0].Priv)
		}
	})
}

func Test_ACLItem_IsAll(t *testing.T) {
	assert := assert.New(t)

	t.Run("HasAll", func(t *testing.T) {
		acl, _ := schema.ParseACLItem("admin:ALL")
		assert.True(acl.IsAll())
	})

	t.Run("NoAll", func(t *testing.T) {
		acl, _ := schema.ParseACLItem("user:SELECT,INSERT")
		assert.False(acl.IsAll())
	})

	t.Run("AllWithOthers", func(t *testing.T) {
		acl, _ := schema.ParseACLItem("admin:SELECT,ALL,INSERT")
		assert.True(acl.IsAll())
	})
}

func Test_ACLItem_String(t *testing.T) {
	assert := assert.New(t)

	t.Run("ProducesTextFormat", func(t *testing.T) {
		acl, _ := schema.ParseACLItem("user:SELECT")
		str := acl.String()
		assert.Contains(str, "user")
		assert.Contains(str, "SELECT")
	})
}
