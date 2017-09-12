package consul

import "context"

// Catalog exposes methods to interract with the consul catalog.
type Catalog struct {
	// The client used by the catalog, which may be nil to indicate that a
	// default client should be used.
	Client *Client
}

// ListServices returns the list of services registered to consul as a map of
// service names to the list of known tags for that service.
func (c *Catalog) ListServices(ctx context.Context) (services map[string][]string, err error) {
	err = c.client().Get(ctx, "/v1/catalog/services", nil, &services)
	return
}

func (c *Catalog) client() *Client {
	if client := c.Client; client != nil {
		return client
	}
	return DefaultClient
}

// DefaultCatalog is a catalog configured to use the default client.
var DefaultCatalog = &Catalog{}

// ListServices is a helper function that delegates to the default catalog.
func ListServices(ctx context.Context) (map[string][]string, error) {
	return DefaultCatalog.ListServices(ctx)
}
