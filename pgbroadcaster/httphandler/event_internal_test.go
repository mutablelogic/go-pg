package httphandler

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	// Packages
	assert "github.com/stretchr/testify/assert"
)

// failingWriter is a response writer whose writes fail, as to a client which
// stopped reading
type failingWriter struct {
	*httptest.ResponseRecorder
}

func (w failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func TestDeadlineWriter(t *testing.T) {
	t.Run("Write", func(t *testing.T) {
		w := newDeadlineWriter(httptest.NewRecorder())
		n, err := w.Write([]byte("data"))
		assert.NoError(t, err)
		assert.Equal(t, 4, n)
		w.Flush()
		select {
		case <-w.failed:
			t.Fatal("a write which succeeded failed the stream")
		default:
		}
	})

	t.Run("Fail", func(t *testing.T) {
		w := newDeadlineWriter(failingWriter{httptest.NewRecorder()})
		_, err := w.Write([]byte("data"))
		assert.Error(t, err)
		_, err = w.Write([]byte("data"))
		assert.Error(t, err)
		select {
		case <-w.failed:
		default:
			t.Fatal("a write which failed didn't fail the stream")
		}
	})

	t.Run("Unwrap", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		w := newDeadlineWriter(recorder)
		assert.Equal(t, http.ResponseWriter(recorder), w.Unwrap())
	})
}
