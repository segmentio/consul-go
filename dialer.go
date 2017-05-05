package consul

import (
	"context"
	"fmt"
	"net"
	"time"
)

// The Dialer type mirrors the net.Dialer API but uses consul to resolve service
// names to network addresses instead of DNS.
//
// The Dialer always ignores ports specified in the addreses that it's trying to
// connect to and uses the ports looked up from consul instead, unless it was
// given and address which is a valid IP representation in which case it does
// not resolve the service name and directly establish the connection.
//
// For a full description of each of the fields please refer to the net.Dialer
// documentation at https://golang.org/pkg/net/#Dialer.
type Dialer struct {
	Timeout       time.Duration
	Deadline      time.Time
	LocalAddr     net.Addr
	DualStack     bool
	FallbackDelay time.Duration
	KeepAlive     time.Duration
	Resolver      *Resolver
}

// Dial establishes a network connection to address, using consul to resolve
// the address if necessary.
//
// For a full description of the method's behavior please refer to the
// (*net.Dialer).Dial documentation at https://golang.org/pkg/net/#Dialer.Dial.
func (d *Dialer) Dial(network string, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

// DialContext establishes a network connection to address, using consul to
// resolve the address if necessary.
//
// For a full description of the method's behavior please refer to the
// (*net.Dialer).Dialcontext documentation at
// https://golang.org/pkg/net/#Dialer.DialContext.
func (d *Dialer) DialContext(ctx context.Context, network string, address string) (net.Conn, error) {
	if host, _ := splitHostPort(address); len(host) != 0 && net.ParseIP(host) == nil {
		addrs, err := d.resolver().LookupService(ctx, host)
		if err != nil {
			return nil, err
		}
		if len(addrs) == 0 {
			return nil, fmt.Errorf("no addresses returned by the resolver for %s", host)
		}
		address = addrs[0]
	}

	return (&net.Dialer{
		Timeout:       d.Timeout,
		Deadline:      d.Deadline,
		LocalAddr:     d.LocalAddr,
		DualStack:     d.DualStack,
		FallbackDelay: d.FallbackDelay,
		KeepAlive:     d.KeepAlive,
	}).DialContext(ctx, network, address)
}

func (d *Dialer) resolver() *Resolver {
	if rslv := d.Resolver; rslv != nil {
		return rslv
	}
	return DefaultResolver
}

// Dial is a wrapper for calling (*Dialer).Dial on a default dialer.
func Dial(network string, address string) (net.Conn, error) {
	return (&Dialer{}).Dial(network, address)
}

// DialContext is a wrapper for calling (*Dialer).DialContext on a default
// dialer.
func DialContext(ctx context.Context, network string, address string) (net.Conn, error) {
	return (&Dialer{}).DialContext(ctx, network, address)
}

func splitHostPort(s string) (string, string) {
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		return s, ""
	}
	return host, port
}
