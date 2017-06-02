package consul

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
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
	ServiceID string

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
	ServiceAddress net.Addr

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

	service := serviceConfig{
		ID:                l.ServiceID,
		Name:              l.ServiceName,
		Tags:              l.ServiceTags,
		EnableTagOverride: l.ServiceEnableTagOverride,
	}

	if service.Name == "" {
		service.Name = filepath.Base(os.Args[0])
	}

	if service.ID == "" {
		service.ID = service.Name
	}

	if l.ServiceAddress != nil {
		service.Address = l.ServiceAddress.String()
		addr, port, _ := net.SplitHostPort(service.Address)
		service.Address = addr
		service.Port, err = strconv.Atoi(port)

		if err != nil {
			err = fmt.Errorf("bad port number in network address %s://%s", network, address)
		}

	} else {
		switch addr := lstn.Addr().(type) {
		case *net.TCPAddr:
			zeroIPv4 := net.ParseIP("0.0.0.0")
			zeroIPv6 := net.ParseIP("::")

			if addr.IP.Equal(zeroIPv4) || addr.IP.Equal(zeroIPv6) {
				addr.IP, err = publicOrLoopbackIP()
				addr.Zone = ""
			}

			service.Address = addr.IP.String()
			service.Port = addr.Port
		default:
			service.Address = addr.String()
		}
	}

	if err != nil {
		lstn.Close()
		return nil, err
	}

	service.Checks = []checkConfig{{
		Notes:    "Ensure consul can establish TCP connections to the service",
		Interval: "10s",
		Status:   "passing",
		TCP:      net.JoinHostPort(service.Address, strconv.Itoa(service.Port)),
	}}

	if l.CheckHTTP != "" {
		service.Checks = append(service.Checks, checkConfig{
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
			service.Checks[i].Interval = seconds(l.CheckInterval)
		}
	}

	if l.CheckDeregisterCriticalServiceAfter != 0 {
		for i := range service.Checks {
			service.Checks[i].DeregisterCriticalServiceAfter = seconds(l.CheckDeregisterCriticalServiceAfter)
		}
	}

	client := l.client()

	if err := client.registerService(ctx, service); err != nil {
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
	serviceID string
	once      sync.Once
}

func (l *listener) Close() error {
	l.once.Do(func() {
		l.client.deregisterService(context.TODO(), l.serviceID)
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

func publicOrLoopbackIP() (net.IP, error) {
	var ifaces []net.Interface
	var err error

	if ifaces, err = net.Interfaces(); err != nil {
		return nil, err
	}
	sort.Slice(ifaces, func(i int, j int) bool { return ifaces[i].Index < ifaces[i].Index })

	for _, iface := range ifaces {
		if (iface.Flags&net.FlagUp) != 0 && (iface.Flags&(net.FlagLoopback|net.FlagPointToPoint)) == 0 {
			if a, e := findIPAddr(iface); e != nil {
				err = e
			} else if a != nil {
				return a, nil
			}
		}
	}

	for _, iface := range ifaces {
		if (iface.Flags & net.FlagLoopback) != 0 {
			if a, e := findIPAddr(iface); e != nil {
				err = e
			} else if a != nil {
				return a, nil
			}
		}
	}

	if err == nil {
		err = errors.New("no network interfaces were found")
	}

	return nil, err
}

func findIPAddr(iface net.Interface) (net.IP, error) {
	addrs, err := iface.Addrs()

	if err != nil {
		return nil, err
	}

	var ipv4 net.IP
	var ipv6 net.IP

	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok {
			if ipnet.IP.To4() != nil {
				ipv4 = ipnet.IP
			} else {
				ipv6 = ipnet.IP
			}
		}
	}

	// Prefer IPv4 because consul has issues with making TCP health checks to
	// IPv6 addresses. If we haven't found a v4 address it's very likely that
	// the health checks won't pass with a v6 address but at least the code is
	// future proof is the program gets resolved.
	if ipv4 != nil {
		return ipv4, nil
	}

	return ipv6, nil
}
