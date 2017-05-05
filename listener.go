package consul

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// Listen creates a listener that accept connections on the given network and
// address, and registers to consul using the default client.
//
// The address may be in a URL format like "host:port/name/id?advertize=address"
// to configure how the listener registers to consul.
// The host, name, id, and advertized address parts are optional.
//
func Listen(network string, address string) (net.Listener, error) {
	return ListenWithClient(network, address, nil)
}

// ListenWithClient creates a listener that accept connections on the given
// network and address, and registers to consul with client.
//
// If client is nil, the default client is used instead.
func ListenWithClient(network string, address string, client *Client) (net.Listener, error) {
	switch network {
	case "tcp", "tcp4", "tcp6":
	default:
		return nil, fmt.Errorf("unsupported network: %s", network)
	}

	config, err := parseAddress(address)
	if err != nil {
		return nil, err
	}

	lstn, err := net.Listen(network, config.address)
	if err != nil {
		return nil, err
	}

	if len(config.advertize) == 0 {
		config.advertize = lstn.Addr().String()
	}

	if client == nil {
		client = DefaultClient
	}

	l := &listener{
		lstn:      lstn,
		client:    client,
		serviceID: ServiceID(config.id),
	}

	if err := l.register(config); err != nil {
		return nil, err
	}

	return l, nil
}

type listener struct {
	lstn      net.Listener
	client    *Client
	serviceID ServiceID
}

func (l *listener) Accept() (net.Conn, error) {
	return l.lstn.Accept()
}

func (l *listener) Addr() net.Addr {
	return l.lstn.Addr()
}

func (l *listener) Close() error {
	l.deregister()
	return l.lstn.Close()
}

func (l *listener) register(config listenConfig) error {
	address, stringPort, err := net.SplitHostPort(config.advertize)
	if err != nil {
		return err
	}

	port, err := strconv.Atoi(stringPort)
	if err != nil {
		return err
	}

	if len(config.id) == 0 {
		config.id = config.name
	}

	if len(config.timeout) == 0 {
		config.timeout = "60m"
	}

	if len(config.notes) == 0 {
		config.notes = "Ensure that consul can establish a TCP connection to the service"
	}

	if len(config.interval) == 0 {
		config.interval = "10s"
	}

	return l.client.RegisterService(context.TODO(), ServiceConfig{
		ID:                ServiceID(config.id),
		Name:              config.name,
		Address:           address,
		Port:              port,
		EnableTagOverride: true,
		Checks: []CheckConfig{{
			DeregisterCriticalServiceAfter: config.timeout,
			TCP:      config.advertize,
			Notes:    config.notes,
			Interval: config.interval,
		}},
	})
}

func (l *listener) deregister() error {
	return l.client.DeregisterService(context.TODO(), l.serviceID)
}

type listenConfig struct {
	address   string
	name      string
	id        string
	advertize string
	timeout   string
	notes     string
	interval  string
}

func parseAddress(s string) (config listenConfig, err error) {
	var u *url.URL

	if u, err = url.Parse(s); err != nil {
		return
	}

	switch path := strings.Split(u.Path, "/"); len(path) {
	case 0:
	case 1:
		config.name = path[0]
	case 2:
		config.name, config.id = path[0], path[1]
	default:
		err = fmt.Errorf("bad listener address: too many path elements: %s", s)
		return
	}

	q := u.Query()

	if config.advertize = q.Get("advertize"); len(config.advertize) != 0 {
		var host string
		var port string
		var v int

		if host, port, err = net.SplitHostPort(config.advertize); err != nil {
			err = fmt.Errorf("bad listener address: invalid advertized address: %s", err)
			return
		}

		if net.ParseIP(host) == nil {
			err = fmt.Errorf("bad listener address: the advertized address must be a valid IP: %s", s)
			return
		}

		if v, err = strconv.Atoi(port); err != nil {
			err = fmt.Errorf("bad listener address: non-numeric port in advertized address: %s", err)
			return
		} else if v < 0 {
			err = fmt.Errorf("bad listener address: negative port number in advertized address: %s", s)
			return
		}
	}

	config.timeout = q.Get("deregister_critical_service_after")
	config.notes = q.Get("notes")
	config.interval = q.Get("interval")
	return
}
