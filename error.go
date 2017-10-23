package consul

import (
	"fmt"
	"net/http"
	"net/url"
)

// IsNotFound reports whether the error is a not found error.
func IsNotFound(err error) bool {
	if nf, ok := err.(notFound); ok {
		return nf.NotFound()
	}
	return false
}

type notFound interface {
	NotFound() bool
}

type baseError struct {
	method     string
	url        *url.URL
	status     string
	statusCode int
}

func (e *baseError) Error() string {
	return fmt.Sprintf("%s %s: %s", e.method, e.url, e.status)
}

func (e *baseError) NotFound() bool {
	return e.statusCode == 404
}

func newRequestError(method string, u *url.URL, res *http.Response) error {
	return &baseError{
		method:     method,
		url:        u,
		status:     res.Status,
		statusCode: res.StatusCode,
	}
}
