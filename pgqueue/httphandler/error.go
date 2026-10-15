package httphandler

import (
	"errors"

	// Packages
	pg "github.com/mutablelogic/go-pg"
	httpresponse "github.com/mutablelogic/go-server/pkg/httpresponse"
)

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// httpErr returns the error with its HTTP status: an error which has one
// already, such as an invalid name, is returned as it is, and a database
// error gets the status for it
func httpErr(err error) error {
	var code httpresponse.Err
	if errors.As(err, &code) {
		return err
	}
	return pg.HTTPError(err)
}
