package consul

import (
	"context"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// The Listener type contains options to create listeners that automatically
// register to consul.
type Listener struct {
	// The Client used to register the listener to a consul agent. If nil, the
	// default client is used instead.
	Client *Client

	// A unique identifier for the service registered to consul. This only needs
	// to be unique within the agent that the service registers to, and may be
	// omitted. In that case, ServiceName is used instead.
	ServiceID ServiceID

	// The logical name of the service registered to consul. If none is set, the
	// program name is used instead.
	ServiceName string

	// A list of tags to set on the service registered to consul.
	ServiceTags []string

	// If non-nil, specifies the address under which the service will be
	// registered to consul. This is useful when running within a container for
	// example, where the program may not have access to the external address to
	// which clients should connect to in order to reach the service.
	// By default, the address that new listeners accept connections on is used.
	ServiceAddress net.IP

	// If ServiceAddress is set, ServicePort should also be set to a non-zero
	// value.
	ServicePort int

	// Configures whether registering the service with specific tags should
	// overwrite existing values.
	ServiceEnableTagOverride bool

	// If the listener is intended to be used to serve HTTP connection this
	// field may be set to the path that consul should query to health check
	// the service.
	CheckHTTP string

	// CheckInterval is the time interval set on the TCP check that is
	// registered to consul along with the service. Defaults to 10 seconds.
	CheckInterval time.Duration

	// Amount of time after which a service that is reported critical should be
	// automatically deregistered. Defaults to never.
	CheckDeregisterCriticalServiceAfter time.Duration
}

// Listen creates a new listener that accepts network connections on the given
// address, and automatically registers to consul.
func (l *Listener) Listen(network string, address string) (net.Listener, error) {
	return l.ListenContext(context.Background(), network, address)
}

// ListenContext creates a new listener that accepts network connections on the
// given address, and automatically registers to consul.
//
// The context may be used to asynchronously cancel the consul registration.
func (l *Listener) ListenContext(ctx context.Context, network string, address string) (net.Listener, error) {
	lstn, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}

	service := ServiceConfig{
		ID:                l.ServiceID,
		Name:              l.ServiceName,
		Tags:              l.ServiceTags,
		Address:           l.ServiceAddress.String(),
		Port:              l.ServicePort,
		EnableTagOverride: l.ServiceEnableTagOverride,
	}

	if service.Name == "" {
		service.Name = filepath.Base(os.Args[0])
	}

	if service.ID == "" {
		service.ID = ServiceID(service.Name)
	}

	if service.Address == "<nil>" {
		addr, port, _ := net.SplitHostPort(lstn.Addr().String())
		service.Address = addr
		service.Port, _ = strconv.Atoi(port)
	}

	service.Checks = []CheckConfig{{
		Notes:    "Ensure consul can establish TCP connections to the service",
		Interval: "10s",
		Status:   "passing",
		TCP:      net.JoinHostPort(service.Address, strconv.Itoa(service.Port)),
	}}

	if l.CheckHTTP != "" {
		service.Checks = append(service.Checks, CheckConfig{
			Notes:    "Ensure consul can submit HTTP requests to the service",
			Interval: "10s",
			Status:   "passing",
			HTTP: (&url.URL{
				Scheme: "http",
				Host:   service.Checks[0].TCP,
				Path:   l.CheckHTTP,
			}).String(),
		})
	}

	if l.CheckInterval != 0 {
		for i := range service.Checks {
			service.Checks[i].Interval = S(l.CheckInterval)
		}
	}

	if l.CheckDeregisterCriticalServiceAfter != 0 {
		for i := range service.Checks {
			service.Checks[i].DeregisterCriticalServiceAfter = S(l.CheckDeregisterCriticalServiceAfter)
		}
	}

	client := l.client()

	if err := client.RegisterService(ctx, service); err != nil {
		lstn.Close()
		return nil, err
	}

	wrap := &listener{
		Listener:  lstn,
		client:    client,
		serviceID: service.ID,
	}

	runtime.SetFinalizer(wrap, (*listener).finalize)
	return wrap, nil
}

func (l *Listener) client() *Client {
	if client := l.Client; client != nil {
		return client
	}
	return DefaultClient
}

type listener struct {
	net.Listener
	client    *Client
	serviceID ServiceID
	once      sync.Once
}

func (l *listener) Close() error {
	l.once.Do(func() {
		l.client.DeregisterService(context.TODO(), l.serviceID)
	})
	return l.Listener.Close()
}

func (l *listener) finalize() {
	l.Close()
}

// Listen creates a listener that accept connections on the given network and
// address, and registers to consul using the default client.
func Listen(network string, address string) (net.Listener, error) {
	return (&Listener{}).Listen(network, address)
}

// ListenContext creates a listener that accept connections on the given network
// and address, and registers to consul use the default client.
//
// The context may be used to asynchronously cancel the consul registration.
func ListenContext(ctx context.Context, network string, address string) (net.Listener, error) {
	return (&Listener{}).ListenContext(ctx, network, address)
}
