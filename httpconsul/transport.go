package httpconsul

import (
	"fmt"
	"net"
	"net/http"
	"time"

	consul "github.com/segmentio/consul-go"
)

// NewTransport returns a decorated version of t that uses consul to resolve the
// service name that requests are being sent for using the default resolver.
//
//	import (
//		"net/http"
//		"github.com/segmentio/consul-go/httpconsul"
//	)
//	// Wraps the default transport so all service names are looked up in consul.
//	// The consul client uses its own transport so there's no risk of recursive
//	// loop here.
//	func init() {
//		http.DefaultTransport = httpconsul.NewTransport(http.DefaultTransport)
//	}
//
func NewTransport(t http.RoundTripper) http.RoundTripper {
	return NewTransportWith(t, consul.DefaultResolver)
}

// NewTransport returns a decorated version of t that uses consul to resolve the
// service name that requests are being sent for using the given resolver.
func NewTransportWith(t http.RoundTripper, r *consul.Resolver) http.RoundTripper {
	return &transport{
		base: t,
		rslv: r,
	}
}

type transport struct {
	base http.RoundTripper
	rslv *consul.Resolver
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	host, _ := splitHostPort(req.URL.Host)
	resolve := len(host) != 0 && net.ParseIP(host) == nil
	attempt := 0

	if !resolve {
		return t.base.RoundTrip(req)
	}

	for {
		addrs, err := t.rslv.LookupService(req.Context(), host)
		if err != nil {
			return nil, err
		}
		if len(addrs) == 0 {
			return nil, fmt.Errorf("no addresses returned by the resolver for %s", host)
		}
		if len(req.Host) == 0 {
			req.Host = req.URL.Host
		}
		req.URL.Host = addrs[0].Addr.String()
		res, err := t.base.RoundTrip(req)

		if err != nil && attempt < 10 && t.rslv.Blacklist != nil && isIdempotent(req.Method) {
			// TODO: make the blacklist TTL configurable here?
			t.rslv.Blacklist.Blacklist(addrs[0].Addr.String(), time.Now().Add(1*time.Second))
			attempt++
			continue
		}

		return res, err
	}
}

func splitHostPort(s string) (string, string) {
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		return s, ""
	}
	return host, port
}

func isIdempotent(method string) bool {
	switch method {
	case "GET", "HEAD", "PUT", "DELETE", "OPTIONS":
		return true
	}
	return false
}
