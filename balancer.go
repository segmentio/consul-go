package consul

import (
	"io/ioutil"
	"net/http"
	"os"
	"strings"
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
	// Constructs the balancing algorithm used by this load balancer.
	//
	// The function cannot be nil or calling the LoadBalancer's Balance method
	// will panic.
	New func() Balancer

	mutex    sync.RWMutex
	version  uint64
	cleaning uint64
	services map[string]*loadBalancerEntry
}

// NewLoadBalancer creates and returns a new instance of LoadBalancer which
// reorders the list of service endpoints using the list of algorithms passed
// as arguments. Valid balancing algorithm names are "round-robin",
// "prefer-ec2-zone", "shuffle", and "weighted-shuffle-on-rtt", or any other
// algorithm which has been registered in the Balancers map. Invalid names
// are ignored, if there are no valid algorithms then round robin is used.
func NewLoadBalancer(algorithms ...string) *LoadBalancer {
	constructors := make([](func() Balancer), 0, len(algorithms))

	for _, a := range algorithms {
		if c, ok := Balancers[a]; ok {
			constructors = append(constructors, c)
		}
	}

	if len(constructors) == 0 {
		constructors = append(constructors, func() Balancer { return &RoundRobin{} })
	}

	return &LoadBalancer{
		New: func() Balancer {
			balancers := make([]Balancer, len(constructors))
			for i, constructor := range constructors {
				balancers[i] = constructor()
			}
			return MultiBalancer(balancers...)
		},
	}
}

type loadBalancerEntry struct {
	Balancer
	version uint64
}

const (
	loadBalancerCleanupInterval = 1000
)

// Balance satisfies the Balancer interface.
func (lb *LoadBalancer) Balance(name string, endpoints []Endpoint) []Endpoint {
	version := atomic.AddUint64(&lb.version, 1)

	lb.mutex.RLock()
	entry := lb.services[name]
	lb.mutex.RUnlock()

	if entry == nil {
		entry = &loadBalancerEntry{
			Balancer: lb.New(),
			version:  version,
		}
		lb.mutex.Lock()
		if lb.services == nil {
			lb.services = make(map[string]*loadBalancerEntry)
		}
		// Don't re-check if the service already exists, worst case we reset the
		// balancer for that service which is fine, in most case it'll make the
		// synchronized section a bit shorter.
		lb.services[name] = entry
		lb.mutex.Unlock()
	}

	endpoints = entry.Balance(name, endpoints)

	if (version % loadBalancerCleanupInterval) == 0 {
		if atomic.CompareAndSwapUint64(&lb.cleaning, 0, 1) {
			lb.cleanup(version)
			atomic.StoreUint64(&lb.cleaning, 0)
		}
	}

	return endpoints
}

func (lb *LoadBalancer) cleanup(version uint64) {
	lb.mutex.RLock()

	for name, entry := range lb.services {
		if diffU64(version, entry.version) > loadBalancerCleanupInterval {
			lb.mutex.RUnlock() // wish there was a way to promote to a write-lock
			lb.mutex.Lock()

			if diffU64(version, entry.version) > loadBalancerCleanupInterval {
				delete(lb.services, name)
			}

			lb.mutex.Unlock()
			lb.mutex.RLock()
		}
	}

	lb.mutex.RUnlock()
}

func diffU64(high uint64, low uint64) uint64 {
	if high < low {
		return 0
	}
	return high - low
}

// RoundRobin is the implementation of a simple load balancing algorithms which
// reorders the slice of endpoints passed to its Balance method in a round robin
// fashion.
type RoundRobin struct {
	offset uint64
}

// Balance satisfies the Balancer interface.
func (rr *RoundRobin) Balance(name string, endpoints []Endpoint) []Endpoint {
	rotated := endpoints

	n := len(endpoints)
	i := int(atomic.AddUint64(&rr.offset, 1) % uint64(n))

	if i != 0 {
		rotate(endpoints, i)
	}

	return rotated
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
	c := &http.Client{
		Transport: DefaultClient.Transport,
	}

	r, err := c.Get("http://169.254.169.254/latest/meta-data/placement/availability-zone")
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

// Balancers is a map of load balancing algorithm names to constructors.
// By default it contains the algorithms defined in this package, but programs
// may modify this map to change the list of algorithms supported by the
// NewLoadBalancer function.
var Balancers = map[string](func() Balancer){
	"round-robin": func() Balancer {
		return &RoundRobin{}
	},

	"prefer-ec2-zone": func() Balancer {
		b, err := PreferEC2AvailabilityZone()
		if err != nil {
			b = BalancerFunc(func(_ string, e []Endpoint) []Endpoint { return e })
		}
		return b
	},

	"shuffle": func() Balancer {
		return &Shuffler{}
	},

	"weighted-shuffle-on-rtt": func() Balancer {
		return &WeightedShuffler{WeightOf: WeightRTT}
	},
}

// DefaultBalancer is the balancer used by the default resolver. The balancer
// is configured based on the value of the CONSUL_LOAD_BALANCER environment
// variable which is expected to be set to a '+' separated list of load balancer
// names.
//
// If CONSUL_LOAD_BALANCER isn't set the default balancer uses a simple
// round-robinn algorithm.
//
// Refer to the NewLoadBalancer documentation for a list of valid names.
var DefaultBalancer Balancer = NewLoadBalancer(strings.Split(os.Getenv("CONSUL_LOAD_BALANCER"), "+")...)
