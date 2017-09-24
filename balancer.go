package consul

import (
	"io/ioutil"
	"net/http"
	"os"
	"sync/atomic"
	"time"
	"unsafe"
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
	// Constructs the balancing algorithm used by this load balancer.
	//
	// The function cannot be nil or calling the LoadBalancer's Balance method
	// will panic.
	New func() Balancer

	version  uint64
	cleaning uint64
	services unsafe.Pointer // *balancerCache
}

const (
	loadBalancerCleanupInterval = 1000
)

// Balance satisfies the Balancer interface.
func (lb *LoadBalancer) Balance(name string, endpoints []Endpoint) []Endpoint {
	entry := lb.cache()[name]

	if entry == nil {
		// Slow path: add the entry to the cache and potentially run a cleanup
		// operation.
		version := atomic.AddUint64(&lb.version, 1)

		entry = &balancerEntry{
			Balancer: lb.New(),
			version:  version,
		}

		for {
			oldCache := lb.loadCache()
			newCache := oldCache.copy()
			newCache[name] = entry

			if lb.compareAndSwapCache(oldCache, &newCache) {
				break
			}
		}

		if (version % loadBalancerCleanupInterval) == 0 {
			if atomic.CompareAndSwapUint64(&lb.cleaning, 0, 1) {
				lb.cleanup(version)
				atomic.StoreUint64(&lb.cleaning, 0)
			}
		}
	}

	// Make the entry as used on the current version of the load balancer cache.
	// It's OK to be eventually consistent here, this is only used to do memory
	// management and evict old entries, if it's off by 1 or 2 it won't make a
	// difference.
	atomic.StoreUint64(&entry.version, atomic.LoadUint64(&lb.version))
	return entry.Balance(name, endpoints)
}

func (lb *LoadBalancer) cleanup(version uint64) {
	minVersion := version - loadBalancerCleanupInterval

	for {
		oldCache := lb.loadCache()
		newCache := oldCache.copy()

		for name, entry := range newCache {
			if entry.version < minVersion {
				delete(newCache, name)
			}
		}

		if lb.compareAndSwapCache(oldCache, &newCache) {
			break
		}
	}
}

func (lb *LoadBalancer) cache() balancerCache {
	cache := lb.loadCache()
	if cache == nil {
		return nil
	}
	return *cache
}

func (lb *LoadBalancer) loadCache() *balancerCache {
	return (*balancerCache)(atomic.LoadPointer(&lb.services))
}

func (lb *LoadBalancer) compareAndSwapCache(oldCache *balancerCache, newCache *balancerCache) bool {
	return atomic.CompareAndSwapPointer(&lb.services, unsafe.Pointer(oldCache), unsafe.Pointer(newCache))
}

func diffU64(high uint64, low uint64) uint64 {
	if high < low {
		return 0
	}
	return high - low
}

type balancerCache map[string]*balancerEntry

func (b *balancerCache) copy() balancerCache {
	c := make(balancerCache)
	if b != nil {
		for k, v := range *b {
			c[k] = v
		}
	}
	return c
}

type balancerEntry struct {
	Balancer
	version uint64
}

// RoundRobin is the implementation of a simple load balancing algorithms which
// chooses and returns a single endpoint of the input list in a round robin
// fashion.
type RoundRobin struct {
	offset uint64
}

// Balance satisfies the Balancer interface.
func (rr *RoundRobin) Balance(name string, endpoints []Endpoint) []Endpoint {
	n := len(endpoints)
	if n == 0 {
		return endpoints
	}
	i := int(atomic.AddUint64(&rr.offset, 1) % uint64(n))
	return endpoints[i : i+1]
}

// Rotator is the implementation of a load balancing algorithms similar to
// RoundRobin but which returns the full list of endpoints instead of a single
// one.
//
// Using this balancer is useful to take advantage of the automatic retry logic
// implemented in the dialer or http transport.
type Rotator struct {
	offset uint64
}

// Balance satisfies the Balancer interface.
func (rr *Rotator) Balance(name string, endpoints []Endpoint) []Endpoint {
	if n := len(endpoints); n != 0 {
		if i := int(atomic.AddUint64(&rr.offset, 1) % uint64(n)); i != 0 {
			rotate(endpoints, i)
		}
	}
	return endpoints
}

func rotate(endpoints []Endpoint, d int) {
	reverse(endpoints[:d])
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
//
// If the metadata aren't available the function returns the NullBalancer
// balancer which doesn't modify the list of endpoints.
func PreferEC2AvailabilityZone(c *http.Client) Balancer {
	if c == nil {
		c = &http.Client{
			Timeout:   5 * time.Second,
			Transport: DefaultClient.Transport,
		}
	}

	r, err := c.Get("http://169.254.169.254/latest/meta-data/placement/availability-zone")
	if err != nil {
		return &NullBalancer{}
	}
	defer r.Body.Close()

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return &NullBalancer{}
	}

	return PreferTags{string(b)}
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

// NullBalancer is a balancer which doesn't modify the list of endpoints.
type NullBalancer struct{}

// Balance satisfies the Balancer interface.
func (*NullBalancer) Balance(name string, endpoints []Endpoint) []Endpoint {
	return endpoints
}

func defaultCacheBalancer() Balancer {
	switch os.Getenv("CONSUL_CACHE_BALANCER") {
	case "ec2-zone-affinity":
		return PreferEC2AvailabilityZone(nil)
	default:
		return &NullBalancer{}
	}
}
