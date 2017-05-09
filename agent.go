package consul

import (
	"context"
	"time"
)

// Agent exposes methods to get information about the agent that a client is
// configured to connect to.
type Agent struct {
	// Client
	Client *Client

	// Configures how often the state is updated. If zero, the state is updated
	// every second.
	Timeout time.Duration

	// Cached agent configuration.
	config cachedValue
}

// NodeName fetches the name of the node that the consul agent is running on.
func (a *Agent) NodeName(ctx context.Context) (string, error) {
	c, err := a.load(ctx)
	if err != nil {
		return "", err
	}
	return c.Config.NodeName, nil
}

func (a *Agent) client() *Client {
	if client := a.Client; client != nil {
		return client
	}
	return DefaultClient
}

func (a *Agent) timeout() time.Duration {
	if timeout := a.Timeout; timeout != 0 {
		return timeout
	}
	return 1 * time.Second
}

func (a *Agent) load(ctx context.Context) (*agentConfig, error) {
	now := time.Now()
	exp := now.Add(a.timeout())

	val, err := a.config.lookup(now, exp, func() (interface{}, error) {
		config, err := a.client().agentConfig(ctx)
		return &config, err
	})

	config, _ := val.(*agentConfig)
	return config, err
}

// DefaultAgent is an agent configured to expose the agent information of the
// consul agent that the default client connects to.
var DefaultAgent = &Agent{}

type agentConfig struct {
	// This is a partial definition of the agent configuration object returned
	// in response to a call to /v1/agent/self.
	Config struct {
		NodeName string
	}
}

type serviceConfig struct {
	ID                string        `json:",omitempty"`
	Name              string        `json:",omitempty"`
	Tags              []string      `json:",omitempty"`
	Address           string        `json:",omitempty"`
	Port              int           `json:",omitempty"`
	EnableTagOverride bool          `json:",omitempty"`
	Checks            []checkConfig `json:",omitempty"`
}

type checkConfig struct {
	ID                             string `json:",omitempty"`
	Name                           string `json:",omitempty"`
	Notes                          string `json:",omitempty"`
	DeregisterCriticalServiceAfter string `json:",omitempty"`
	Script                         string `json:",omitempty"`
	DockerContainerID              string `json:",omitempty"`
	Shell                          string `json:",omitempty"`
	HTTP                           string `json:",omitempty"`
	TCP                            string `json:",omitempty"`
	Interval                       string `json:",omitempty"`
	TTL                            string `json:",omitempty"`
	Status                         string `json:",omitempty"`
	ServiceID                      string `json:",omitempty"`
	TLSSkipVerify                  bool   `json:",omitempty"`
}

func (c *Client) agentConfig(ctx context.Context) (config agentConfig, err error) {
	err = c.Get(ctx, "/v1/agent/self", nil, &config)
	return
}

func (c *Client) registerService(ctx context.Context, service serviceConfig) (err error) {
	err = c.Put(ctx, "/v1/agent/service/register", nil, service, nil)
	return
}

func (c *Client) deregisterService(ctx context.Context, service string) (err error) {
	err = c.Put(ctx, "/v1/agent/service/deregister/"+string(service), nil, nil, nil)
	return
}
