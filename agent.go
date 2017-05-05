package consul

import "context"

// ServiceID represents a unique identifier for services.
type ServiceID string

// ServiceConfig is used to configure how a service is registered.
type ServiceConfig struct {
	ID                ServiceID     `json:",omitempty"`
	Name              string        `json:",omitempty"`
	Tags              []string      `json:",omitempty"`
	Address           string        `json:",omitempty"`
	Port              int           `json:",omitempty"`
	EnableTagOverride bool          `json:",omitempty"`
	Checks            []CheckConfig `json:",omitempty"`
}

// CheckID represents a unique identifier for health checks.
type CheckID string

// CheckStatus is an enumeration of the various states of the health check.
type CheckStatus string

const (
	// Passing indicates that a health check has detected no issues.
	Passing CheckStatus = "passing"

	// Warning indicates that a health check has detected issues but the
	// the service can still operate.
	Warning CheckStatus = "warning"

	// Critical indicates that a health check has failed and recovery requires
	// human intervention.
	Critical CheckStatus = "critical"
)

// CheckConfig is used to configure the checks on a service being registered.
type CheckConfig struct {
	ID                             CheckID     `json:",omitempty"`
	Name                           string      `json:",omitempty"`
	Notes                          string      `json:",omitempty"`
	DeregisterCriticalServiceAfter string      `json:",omitempty"`
	Script                         string      `json:",omitempty"`
	DockerContainerID              string      `json:",omitempty"`
	Shell                          string      `json:",omitempty"`
	HTTP                           string      `json:",omitempty"`
	TCP                            string      `json:",omitempty"`
	Interval                       string      `json:",omitempty"`
	TTL                            string      `json:",omitempty"`
	Status                         CheckStatus `json:",omitempty"`
	ServiceID                      ServiceID   `json:",omitempty"`
	TLSSkipVerify                  bool        `json:",omitempty"`
}

// RegisterService registers a service to the consul agent.
func (c *Client) RegisterService(ctx context.Context, service ServiceConfig) (err error) {
	err = c.Put(ctx, "/v1/agent/service/register", nil, service, nil)
	return
}

// DeregisterService deregisters a service from the consul agent.
func (c *Client) DeregisterService(ctx context.Context, service ServiceID) (err error) {
	err = c.Put(ctx, "/v1/agent/service/deregister/"+string(service), nil, nil, nil)
	return
}
