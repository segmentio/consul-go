package consul

import (
	"math"
	"math/rand"
	"net"
	"sort"
	"sync/atomic"
	"time"
)

// An Endpoint represents the address at which a service is available, coupled
// with the metadata associated with the service registration.
type Endpoint struct {
	// The ID under which the service was registered.
	ID string

	// The node name that the endpoint belongs to.
	Node string

	// The network address at which the service can be reached.
	Addr net.Addr

	// The list of tags associated with the service.
	Tags []string

	// The set of metadata associated with the node on which the service is
	// running.
	Meta map[string]string

	// RTT is an estimation of the round-trip-time between the node specified by
	// Resolver.Agent and the endpoint (may be zero if the information wasn't yet
	// available).
	RTT time.Duration

	// This field is used internally by the weighted shuffle algorithms,
	// embedding it in the endpoint value itself makes the algorithm more
	// efficient since it doesn't need to allocate a separate slice to do the
	// shuffling and then map the results back to the endpoint list.
	expWeight float64

	// TODO: add health check information?
}

// Shuffle is a sorting function that randomly rearranges the list of endpoints.
func Shuffle(list []Endpoint) {
	for i := range list {
		j := rand.Intn(i + 1)
		list[i], list[j] = list[j], list[i]
	}
}

// WeightedShuffleOnRTT is a sorting function that randomly rearranges the list
// of endpoints, using the RTT as a weight to increase the chance of endpoints
// with low RTT to be placed at the front of the list.
func WeightedShuffleOnRTT(list []Endpoint) {
	WeightedShuffle(list, func(endpoint Endpoint) float64 {
		if endpoint.RTT != 0 {
			return float64(endpoint.RTT)
		}
		// If the RTT information was not available there are typically three
		// situations:
		//
		// - The coordinates were not available yet to do caching of the
		// tomography information, in that case we're better off delaying
		// traffic from reaching the endpoint until the tomography is updated.
		//
		// - There was an error getting the tomography information, this is very
		// unlikely since it only needs to be fetched once (the cache is never
		// expired if it can't be updated). In that case it's very likely that
		// all endpoints will have a zero RTT and using a non-zero weight will
		// help shuffle the list of endpoints.
		//
		// - The list of endpoints doesn't come from Resolver.LookupService and
		// no RTT has been configured. Again, using a non-zero weight helps the
		// weighted shuffled algorithm.
		return math.MaxFloat64
	})
}

// WeightedShuffle is a sorting function that randomly rearranges the list of
// endpoints, using the weightOf function to obtain the weight of each endpoint
// of the list.
func WeightedShuffle(list []Endpoint, weightOf func(Endpoint) float64) {
	for i := range list {
		list[i].expWeight = weightOf(list[i]) * expFloat64()
	}
	sort.Sort(byExpWeight(list))
}

type byExpWeight []Endpoint

func (list byExpWeight) Len() int {
	return len(list)
}

func (list byExpWeight) Less(i int, j int) bool {
	return list[i].expWeight < list[j].expWeight
}

func (list byExpWeight) Swap(i int, j int) {
	list[i], list[j] = list[j], list[i]
}

func init() {
	// We cache a bunch of pregenerated exp float64 to use in WeightedShuffle
	// because the rand.ExpFloat64 function uses a global RNG which synchronizes
	// random number generations on a mutex which easily becomes a heavy point
	// of contention in applications that use the consul resolver frequently.
	r := rand.New(rand.NewSource(time.Now().Unix()))

	for i := range randExpFloat64 {
		randExpFloat64[i] = r.ExpFloat64()
	}
}

func expFloat64() float64 {
	return randExpFloat64[atomic.AddUint64(&randOffset, 1)%randValues]
}

const (
	randValues = 255
)

var (
	randOffset     uint64
	randExpFloat64 [randValues]float64
)
