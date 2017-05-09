package consul

import (
	"context"
	"math"
	"time"
)

// The Coordinates type represents network coordinates of nodes in a consul
// system.
//
// More information can be found at:
// https://www.consul.io/docs/internals/coordinates.html
type Coordinates struct {
	Adjustment float64
	Error      float64
	Height     float64
	Vec        [8]float64
}

// Distance computes the approximate RTT between two Consul coordinates.
//
// The algorithm was taken from:
// https://www.consul.io/docs/internals/coordinates.html#working-with-coordinates
func Distance(a Coordinates, b Coordinates) time.Duration {
	// Calculate the Euclidean distance plus the heights.
	sumsq := 0.0
	for i := 0; i < len(a.Vec); i++ {
		diff := a.Vec[i] - b.Vec[i]
		sumsq += diff * diff
	}
	rtt := math.Sqrt(sumsq) + a.Height + b.Height

	// Apply the adjustment components, guarding against negatives.
	adjusted := rtt + a.Adjustment + b.Adjustment
	if adjusted > 0.0 {
		rtt = adjusted
	}

	// Go's times are natively nanoseconds, so we convert from seconds.
	const secondsToNanoseconds = 1.0e9
	return time.Duration(rtt * secondsToNanoseconds)
}

// NodeCoordinates is a mapping of node names to their consul coordinates.
type NodeCoordinates map[string]Coordinates

// Distance computes the approximate RTT between two nodes of coords. The second
// return value is true if both nodes existed in the map, false otherwise.
func (coords NodeCoordinates) Distance(nodeA string, nodeB string) (time.Duration, bool) {
	c1, ok1 := coords[nodeA]
	c2, ok2 := coords[nodeB]
	return Distance(c1, c2), ok1 && ok2
}

// Tomography exposes methods to fetch network coordinates information from
// consul.
//
// The Tomography implementation uses an internal cache of node coordinates that
// is updated asynchronously and fully non-blocking (except for the very first
// call which has to initialize the cache).
//
// Methods of Tomography are safe to use concurrently from multiple goroutines,
// assuming the fields aren't being modified after the value was constructed.
type Tomography struct {
	// The Client used to send requests to a consul agent.
	Client *Client

	// Configures how often the state is updated. If zero, the state is updated
	// every second.
	CacheTimeout time.Duration

	// Cached state of the consul network tomography.
	nodes cachedValue
}

// NodeCoordinates returns the current coordinates of all nodes in the consul
// datacenter.
func (t *Tomography) NodeCoordinates(ctx context.Context) (NodeCoordinates, error) {
	now := time.Now()
	exp := now.Add(t.cacheTimeout())

	val, err := t.nodes.lookup(now, exp, func() (interface{}, error) {
		return t.client().nodeCoordinates(ctx)
	})

	nodes, _ := val.(NodeCoordinates)
	return nodes, err
}

func (t *Tomography) client() *Client {
	if client := t.Client; client != nil {
		return client
	}
	return DefaultClient
}

func (t *Tomography) cacheTimeout() time.Duration {
	if cacheTimeout := t.CacheTimeout; cacheTimeout != 0 {
		return cacheTimeout
	}
	return 1 * time.Second
}

// DefaultTomography is used as the default Tomography instance when
var DefaultTomography = &Tomography{}

func (c *Client) nodeCoordinates(ctx context.Context) (nodes NodeCoordinates, err error) {
	var results []struct {
		Node  string
		Coord Coordinates
	}

	if err = c.Get(ctx, "/v1/coordinate/nodes", nil, &results); err != nil {
		return
	}

	nodes = make(NodeCoordinates, len(results))

	for _, res := range results {
		nodes[res.Node] = res.Coord
	}

	return
}
