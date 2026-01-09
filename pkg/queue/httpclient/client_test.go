package httpclient_test

import (
	"testing"

	// Packages
	httpclient "github.com/mutablelogic/go-pg/pkg/queue/httpclient"
	assert "github.com/stretchr/testify/assert"
)

func Test_Client_New(t *testing.T) {
	assert := assert.New(t)

	t.Run("ValidURL", func(t *testing.T) {
		client, err := httpclient.New("http://localhost:8080")
		assert.NoError(err)
		assert.NotNil(client)
	})

	t.Run("ValidURLWithPath", func(t *testing.T) {
		client, err := httpclient.New("http://localhost:8080/api/v1")
		assert.NoError(err)
		assert.NotNil(client)
	})

	t.Run("ValidHTTPSURL", func(t *testing.T) {
		client, err := httpclient.New("https://example.com")
		assert.NoError(err)
		assert.NotNil(client)
	})

	t.Run("EmptyURL", func(t *testing.T) {
		_, err := httpclient.New("")
		assert.Error(err)
	})
}
