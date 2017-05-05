package consul

import (
	"context"
	"net"
	"strconv"
)

// A Resolver is a high-level abstraction on top of the consul service discovery
// API.
//
// The zero-value is a valid Resolver that uses DefaultClient to query the
// consul agent.
type Resolver struct {
	// The client used by the resolver, which may be nil to indicate that a
	// default client should be used.
	Client *Client

	// Near may be set to the address of a node which is used to sort the list
	// of resolved addresses based on the estimated round trip time from that
	// node.
	//
	// Setting this field to "_agent" will use the consul agent's node for the
	// sort.
	Near string

	// A list of service tags used to filter the result set. Only addresses of
	// services that match those tags will be returned by LookupService.
	ServiceTags []string

	// A set of key/value pairs used to filter the result set. Only addresses of
	// nodes that have matching metadata will be returned by LookupService.
	NodeMeta map[string]string
}

// LookupService resolves a service name to a list of addresses using the
// resolver's configuration and the given list of extra service tags to narrow
// the result set. Only addresses of healthy services are returned by the lookup
// operation.
func (rslv *Resolver) LookupService(ctx context.Context, name string, tags ...string) (addrs []string, err error) {
	var results []struct {
		// There are other fields in the response which have been omitted to
		// avoiding parsing a bunch of throw-away values. Refer to the consul
		// documentation for a full description of the schema:
		// https://www.consul.io/api/health.html#list-nodes-for-service
		Service struct {
			Address string
			Port    int
		}
	}

	query := make(Query, 0, 2+len(rslv.NodeMeta)+len(rslv.ServiceTags)+len(tags))
	query = append(query, Param{Name: "passing"})
	query = queryAppendNodeMeta(query, rslv.NodeMeta)
	query = queryAppendTags(query, rslv.ServiceTags...)
	query = queryAppendTags(query, tags...)

	if near := rslv.Near; len(near) != 0 {
		query = append(query, Param{
			Name:  "near",
			Value: near,
		})
	}

	if err = rslv.client().Get(ctx, "/v1/health/service/"+name, query, &results); err != nil {
		return
	}

	addrs = make([]string, len(results))

	for i, res := range results {
		addrs[i] = net.JoinHostPort(res.Service.Address, strconv.Itoa(res.Service.Port))
	}

	return
}

func (rslv *Resolver) client() *Client {
	if client := rslv.Client; client != nil {
		return client
	}
	return DefaultClient
}

func queryAppendTags(query Query, tags ...string) Query {
	for _, tag := range tags {
		query = append(query, Param{
			Name:  "tag",
			Value: tag,
		})
	}
	return query
}

func queryAppendNodeMeta(query Query, nodeMeta map[string]string) Query {
	for key, value := range nodeMeta {
		query = append(query, Param{
			Name:  "node-meta",
			Value: key + ":" + value,
		})
	}
	return query
}

// DefaultResolver is the Resolver used by a Dialer when non has been specified.
var DefaultResolver = &Resolver{
	Near: "_agent",
}

// LookupService is a wrapper around the default resolver's LookupService
// method.
func LookupService(ctx context.Context, name string, tags ...string) ([]string, error) {
	return DefaultResolver.LookupService(ctx, name, tags...)
}
