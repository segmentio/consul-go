package consul

import (
	"fmt"
	"net/http"
	"net/url"
)

type httpError struct {
	method     string
	url        *url.URL
	status     string
	statusCode int
}

func (e *httpError) Error() string {
	return fmt.Sprintf("%s %s: %s", e.method, e.url, e.status)
}

func (e *httpError) NotFound() bool {
	return e.statusCode == http.StatusNotFound
}

func newHTTPError(method string, u *url.URL, res *http.Response) error {
	return &httpError{
		method:     method,
		url:        u,
		status:     res.Status,
		statusCode: res.StatusCode,
	}
}
