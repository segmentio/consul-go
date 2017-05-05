package consul

import "context"

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

func (c *Client) registerService(ctx context.Context, service serviceConfig) (err error) {
	err = c.Put(ctx, "/v1/agent/service/register", nil, service, nil)
	return
}

func (c *Client) deregisterService(ctx context.Context, service string) (err error) {
	err = c.Put(ctx, "/v1/agent/service/deregister/"+string(service), nil, nil, nil)
	return
}
