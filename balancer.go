package consul

import (
	"io/ioutil"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
)

// Balancer is the interface implemented by types that provides load balancing
// algorithms for a Resolver.
//
// Balancers must be safe to use concurrently from multiple goroutines.
type Balancer interface {
	// Balance is called with a service name and a list of endpoints that this
	// name resolved to, and returns the potentially modified list of endpoints
	// sorted by preference.
	//
	// The returned slice of endpoints may or may not be the same slice than the
	// one that was passed to the method, the balancer implementation is allowed
	// to perform in-place modifications of the endpoints slice, applications
	// must take into consideration that the balancer ownes the slice for the
	// duration of the method call.
	//
	// Balance must not retain the slice of endpoints it received, nor the one
	// it returned.
	Balance(name string, endpoints []Endpoint) []Endpoint
}

// BalancerFunc allows regular functions to be used as balancers.
type BalancerFunc func(string, []Endpoint) []Endpoint

// Balance calls f, satisfies the Balancer interface.
func (f BalancerFunc) Balance(name string, endpoints []Endpoint) []Endpoint {
	return f(name, endpoints)
}

// MultiBalancer composes a new Balancer from a list of multiple balancers, each
// of them being called for each Balance call in the order that they were given
// to the function.
func MultiBalancer(balancers ...Balancer) Balancer {
	multi := make([]Balancer, len(balancers))
	copy(multi, balancers)
	return &multiBalancer{balancers: multi}
}

type multiBalancer struct {
	balancers []Balancer
}

func (m *multiBalancer) Balance(name string, endpoints []Endpoint) []Endpoint {
	for _, b := range m.balancers {
		endpoints = b.Balance(name, endpoints)
	}
	return endpoints
}

// A LoadBalancer is an implementation of Balancer which maintains a set of
// balancers that are local to each service name that Balance has been called
// for.
// It enables using simple load balancing algorithms that are designed to work
// on a set of endpoints belonging to a single service (like RoundRobin) in the
// context of the Resolver which may be used to
type LoadBalancer struct {
	New func() Balancer

	mutex    sync.RWMutex
	version  uint32
	cleaning uint32
	services map[string]*loadBalancerEntry
}

type loadBalancerEntry struct {
	Balancer
	version uint32
}

const (
	loadBalancerCleanupInterval = 1000
)

// Balance satisfies the Balancer interface.
func (lb *LoadBalancer) Balance(name string, endpoints []Endpoint) []Endpoint {
	version := atomic.AddUint32(&lb.version, 1)

	lb.mutex.RLock()
	entry := lb.services[name]
	lb.mutex.RUnlock()

	if entry == nil {
		entry = &loadBalancerEntry{
			Balancer: lb.New(),
			version:  version,
		}
		lb.mutex.Lock()
		// Don't re-check if the service already exists, worst case we reset the
		// balancer for that service which is fine, in most case it'll make the
		// synchronized section a bit shorter.
		lb.services[name] = entry
		lb.mutex.Unlock()
	}

	endpoints = entry.Balance(name, endpoints)

	if (version % loadBalancerCleanupInterval) == 0 {
		if atomic.CompareAndSwapUint32(&lb.cleaning, 0, 1) {
			lb.cleanup(version)
			atomic.StoreUint32(&lb.cleaning, 0)
		}
	}

	return endpoints
}

func (lb *LoadBalancer) cleanup(version uint32) {
	lb.mutex.RLock()

	for name, entry := range lb.services {
		if diffU32(version, entry.version) > loadBalancerCleanupInterval {
			lb.mutex.RUnlock() // wish there was a way to promote to a write-lock
			lb.mutex.Lock()

			if diffU32(version, entry.version) > loadBalancerCleanupInterval {
				delete(lb.services, name)
			}

			lb.mutex.Unlock()
			lb.mutex.RUnlock()
		}
	}

	lb.mutex.RUnlock()
}

func diffU32(high uint32, low uint32) uint32 {
	if high < low {
		return (math.MaxUint32 - high) + low
	}
	return high - low
}

// RoundRobin is the implementation of a simple load balancing algorithms which
// reorders the slice of endpoints passed to its Balance method in a round robin
// fashion.
type RoundRobin struct {
	offset uint32
}

// Balance satisfies the Balancer interface.
func (rr *RoundRobin) Balance(name string, endpoints []Endpoint) []Endpoint {
	rotated := endpoints

	n := len(endpoints)
	i := int(atomic.AddUint32(&rr.offset, 1)) % n

	if i != 0 {
		rotate(endpoints, i)
	}

	return rotated
}

func rotate(endpoints []Endpoint, d int) {
	reverse(endpoints[:d-1])
	reverse(endpoints[d:])
	reverse(endpoints)
}

func reverse(endpoints []Endpoint) {
	i := 0
	j := len(endpoints) - 1

	for i < j {
		swap(endpoints, i, j)
		i++
		j--
	}
}

func swap(endpoints []Endpoint, i int, j int) {
	endpoints[i], endpoints[j] = endpoints[j], endpoints[i]
}

// PreferTags is a balancer which groups endpoints that match certain tags.
// The tags are ordered by preference, so endpoints matching the tag at index
// zero will be placed at the head of the result list.
//
// The result slice is also truncated to return only endpoints that matched at
// least one tag, unless this would end up returning an empty slice, in which
// case the balancer simply returns the full endpoints list.
type PreferTags []string

// Balance satisfies the Balancer interface.
func (tags PreferTags) Balance(name string, endpoints []Endpoint) []Endpoint {
	i := 0
	n := len(endpoints)

	for _, tag := range tags {
		j := 0

		for i < n {
			if containsTag(endpoints[i].Tags, tag) {
				i++
				continue
			}

			if j <= i {
				j = i + 1
			}

			for j < n && !containsTag(endpoints[j].Tags, tag) {
				j++
			}

			if j == n {
				break
			}

			swap(endpoints, i, j)
			j++
			i++
		}
	}

	if i == 0 {
		i = n
	}

	return endpoints[:i]
}

func containsTag(tags []string, tag string) bool {
	for _, candidate := range tags {
		if candidate == tag {
			return true
		}
	}
	return false
}

// PreferEC2AvailabilityZone is a constructor for a balancer which prefers
// routing traffic to services registered in the same EC2 availability zone
// than the caller.
func PreferEC2AvailabilityZone() (Balancer, error) {
	r, err := http.Get("http://169.254.169.254/latest/meta-data/placement/availability-zone")
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	return PreferTags{string(b)}, nil
}

// Shuffler is a Balancer implementation which returns a randomly shuffled list
// of endpoints.
type Shuffler struct{}

// Balance satsifies the Balancer interface.
func (*Shuffler) Balance(name string, endpoints []Endpoint) []Endpoint {
	Shuffle(endpoints)
	return endpoints
}

// WeightedShuffler is a Balancer implementation which shuffles the list of
// endpoints using a different weight for each endpoint.
type WeightedShuffler struct {
	// WeightOf returns the weight of an endpoint.
	WeightOf func(Endpoint) float64
}

// Balance satisfies the Balancer interface.
func (ws *WeightedShuffler) Balance(name string, endpoints []Endpoint) []Endpoint {
	weightOf := ws.WeightOf

	if weightOf == nil {
		weightOf = func(_ Endpoint) float64 { return 1.0 }
	}

	WeightedShuffle(endpoints, weightOf)
	return endpoints
}
