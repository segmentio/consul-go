package consul

import (
	"context"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
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

	// A list of service tags used to filter the result set. Only addresses of
	// services that match those tags will be returned by LookupService.
	ServiceTags []string

	// A set of key/value pairs used to filter the result set. Only addresses of
	// nodes that have matching metadata will be returned by LookupService.
	NodeMeta map[string]string

	// If set to true, the resolver only returns services that are passing their
	// health checks.
	OnlyPassing bool

	// Cache used by the resolver to reduce the number of round-trips to consul.
	// If set to nil then no cache is used.
	//
	// ResolverCache instances should not be shared by multiple resolvers
	// because the cache uses the service name as a lookup key, but the resolver
	// may apply filters based on the values of the ServiceTags, NodeMeta, and
	// OnlyPassing fields.
	Cache *ResolverCache

	// This field may be set to allow the resolver to support temporarily
	// blacklisting addresses that are known to be unreachable.
	Blacklist *ResolverBlacklist

	// Agent is used to set the origin from which the distance to each endpoints
	// are computed. If nil, DefaultAgent is used instead.
	Agent *Agent

	// Tomography is used when Agent is set to compute the distance from the
	// agnet to the endpoints. If nil, DefaultTomography is used instead.
	Tomography *Tomography

	// Sort is called to order the list of endpoints returned by the resolver.
	// Setting this field to nil means no ordering of the endpoints is done.
	//
	// If the resolver is intended to be used to distribute load across a pool
	// of services it is important to set a Sort function that shuffles the list
	// of endpoints, otherwise consecutive calls would likely return the list in
	// the same order, and picking the first item would result in routing all
	// traffic to a single instance of the service.
	Sort func([]Endpoint)
}

// LookupHost resolves a service name to a list of network addresses.
//
// The method name is a bit misleading because it uses the term Host but accepts
// a service name as argument, this is done to match the signature of the
// net.(*Resolver).LookupHost method so the types can satisfy the same interface.
func (rslv *Resolver) LookupHost(ctx context.Context, name string) ([]string, error) {
	endpoints, err := rslv.LookupService(ctx, name)

	if err != nil {
		return nil, err
	}

	addrs := make([]string, len(endpoints))

	for i, endpoint := range endpoints {
		addrs[i] = endpoint.Addr.String()
	}

	return addrs, nil
}

// LookupService resolves a service name to a list of endpoints using the
// resolver's configuration to narrow and sort the result set.
func (rslv *Resolver) LookupService(ctx context.Context, name string) ([]Endpoint, error) {
	var list []Endpoint
	var err error

	serviceName, serviceID := splitNameID(name)

	if cache := rslv.Cache; cache != nil {
		list, err = cache.LookupService(ctx, serviceName, rslv.lookupService)
	} else {
		list, err = rslv.lookupService(ctx, serviceName)
	}

	if err != nil {
		return nil, err
	}

	if rslv.Blacklist != nil {
		list = rslv.Blacklist.Filter(list, time.Now())
	}

	if rslv.Sort != nil {
		rslv.Sort(list)
	}

	if len(serviceID) != 0 {
		i := 0

		for _, e := range list {
			if _, id := splitNameID(e.ID); id == serviceID {
				list[i] = e
				i++
			}
		}

		list = list[:i]
	}

	return list, err
}

func (rslv *Resolver) lookupService(ctx context.Context, name string) (list []Endpoint, err error) {
	var results []struct {
		// There are other fields in the response which have been omitted to
		// avoiding parsing a bunch of throw-away values. Refer to the consul
		// documentation for a full description of the schema:
		// https://www.consul.io/api/health.html#list-nodes-for-service
		Node struct {
			Node string
			Meta map[string]string
		}
		Service struct {
			ID      string
			Address string
			Port    int
			Tags    []string
		}
	}

	query := make(Query, 0, 1+len(rslv.NodeMeta)+len(rslv.ServiceTags))

	if rslv.OnlyPassing {
		query = append(query, Param{Name: "passing"})
	}

	for key, value := range rslv.NodeMeta {
		query = append(query, Param{
			Name:  "node-meta",
			Value: key + ":" + value,
		})
	}

	for _, tag := range rslv.ServiceTags {
		query = append(query, Param{
			Name:  "tag",
			Value: tag,
		})
	}

	if err = rslv.client().Get(ctx, "/v1/health/service/"+name, query, &results); err != nil {
		return
	}

	list = make([]Endpoint, len(results))

	for i, res := range results {
		list[i] = Endpoint{
			ID:   res.Service.ID,
			Addr: newServiceAddr(res.Service.Address, res.Service.Port),
			Tags: res.Service.Tags,
			Node: res.Node.Node,
			Meta: res.Node.Meta,
		}
	}

	agent, _ := rslv.agent().NodeName(ctx)
	nodes, _ := rslv.tomography().NodeCoordinates(ctx)

	if from, ok := nodes[agent]; ok {
		for i := range list {
			if to, ok := nodes[list[i].Node]; ok {
				list[i].RTT = Distance(from, to)
			}
		}
	}

	return
}

func (rslv *Resolver) client() *Client {
	if client := rslv.Client; client != nil {
		return client
	}
	return DefaultClient
}

func (rslv *Resolver) agent() *Agent {
	if agent := rslv.Agent; agent != nil {
		return agent
	}
	return DefaultAgent
}

func (rslv *Resolver) tomography() *Tomography {
	if tomography := rslv.Tomography; tomography != nil {
		return tomography
	}
	return DefaultTomography
}

// DefaultResolver is the Resolver used by a Dialer when non has been specified.
var DefaultResolver = &Resolver{
	OnlyPassing: true,
	Blacklist:   &ResolverBlacklist{},
	Sort:        WeightedShuffleOnRTT,
}

// LookupService is a wrapper around the default resolver's LookupService
// method.
func LookupService(ctx context.Context, name string) ([]Endpoint, error) {
	return DefaultResolver.LookupService(ctx, name)
}

type serviceAddr string

func newServiceAddr(host string, port int) serviceAddr {
	return serviceAddr(net.JoinHostPort(host, strconv.Itoa(port)))
}

func (serviceAddr) Network() string  { return "" }
func (a serviceAddr) String() string { return string(a) }

// LookupServiceFunc is the signature of functions that can be used to lookup
// service names.
type LookupServiceFunc func(context.Context, string) ([]Endpoint, error)

// The ResolverCache type provides the implementation of a caching layer for
// service name resolutions.
//
// Instances of ResolverCache are save to use concurrently from multiple
// goroutines.
type ResolverCache struct {
	// The maximum age of cache entries. If zero, a default value of 1 second is
	// used.
	CacheTimeout time.Duration

	once sync.Once
	cmap *resolverCacheMap
}

// LookupService resolves a service name by fetching the address list from the
// cache, or calling lookup if the name did not exist.
func (cache *ResolverCache) LookupService(ctx context.Context, name string, lookup LookupServiceFunc) ([]Endpoint, error) {
	cache.once.Do(cache.init)

	now := time.Now()
	cacheTimeout := cache.cacheTimeout()
	entry, locked := cache.cmap.lookup(name, now, now.Add(cacheTimeout))

	if locked {
		// TODO: check the error type here and discard things like context
		// cancellations and timeouts?
		entry.res, entry.err = lookup(ctx, name)
		entry.mutex.Unlock()
	}

	entry.mutex.RLock()
	res, err, expireAt := entry.res, entry.err, entry.expireAt
	entry.mutex.RUnlock()

	// To reduce the chances of getting cache misses on expired entries we
	// prefetch the updated list of addresses when we're getting close to the
	// expiration time. This is not a perfect solution and works when fetching
	// the address list completes before the cleanup goroutine gets rid of the
	// cache entry, but it has the advantage of being a fully non-blocking
	// approach.
	if now.After(expireAt.Add(-cacheTimeout / 10)) {
		if atomic.CompareAndSwapUint32(&entry.lock, 0, 1) {
			res, err := lookup(ctx, name)
			exp := time.Now().Add(cacheTimeout)

			entry.mutex.Lock()
			entry.res = res
			entry.err = err
			entry.expireAt = exp
			entry.mutex.Unlock()

			atomic.StoreUint32(&entry.lock, 0)
		}
	}

	// We have to make a copy to let the caller own the value returned by this
	// method. Otherwise it could make changes that modify the cache's internal
	// memory, which would cause races and unexpected behaviors between calls to
	// the LookupService method.
	list := make([]Endpoint, len(res))
	copy(list, res)
	return list, err
}

func (cache *ResolverCache) init() {
	cache.cmap = &resolverCacheMap{
		entries: make(map[string]*resolverCacheEntry),
	}

	ctx, cancel := context.WithCancel(context.Background())
	runtime.SetFinalizer(cache, func(_ *ResolverCache) { cancel() })

	interval := cache.cacheTimeout() / 2
	go cache.cmap.autoDeleteExpired(ctx, interval)
}

func (cache *ResolverCache) cacheTimeout() time.Duration {
	if cacheTimeout := cache.CacheTimeout; cacheTimeout != 0 {
		return cache.CacheTimeout
	}
	return 1 * time.Second
}

type resolverCacheMap struct {
	mutex   sync.RWMutex
	entries map[string]*resolverCacheEntry
}

type resolverCacheEntry struct {
	mutex    sync.RWMutex
	lock     uint32
	name     string
	res      []Endpoint
	err      error
	expireAt time.Time
}

func (cmap *resolverCacheMap) lookup(name string, now time.Time, exp time.Time) (entry *resolverCacheEntry, locked bool) {
	cmap.mutex.RLock()
	entry = cmap.entries[name]
	cmap.mutex.RUnlock()

	if entry == nil {
		cmap.mutex.Lock()

		if entry = cmap.entries[name]; entry == nil {
			entry, locked = &resolverCacheEntry{name: name, expireAt: exp}, true
			entry.mutex.Lock()
			cmap.entries[name] = entry
		}

		cmap.mutex.Unlock()
	}

	return
}

func (cmap *resolverCacheMap) delete(entry *resolverCacheEntry) {
	cmap.mutex.Lock()

	if cmap.entries[entry.name] == entry {
		delete(cmap.entries, entry.name)
	}

	cmap.mutex.Unlock()
}

func (cmap *resolverCacheMap) autoDeleteExpired(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			cmap.deleteExpired(now)
		}
	}
}

func (cmap *resolverCacheMap) deleteExpired(now time.Time) {
	for _, entry := range cmap.listExpired(now) {
		cmap.delete(entry)
	}
}

func (cmap *resolverCacheMap) listExpired(now time.Time) []*resolverCacheEntry {
	entries := cmap.list()
	i := 0

	for _, entry := range entries {
		entries[i] = entry
		entry.mutex.RLock()
		if now.After(entry.expireAt) {
			i++
		}
		entry.mutex.RUnlock()
	}

	return entries[:i]
}

func (cmap *resolverCacheMap) list() []*resolverCacheEntry {
	cmap.mutex.RLock()
	entries := make([]*resolverCacheEntry, 0, len(cmap.entries))

	for _, entry := range cmap.entries {
		entries = append(entries, entry)
	}

	cmap.mutex.RUnlock()
	return entries
}

func splitNameID(s string) (name string, id string) {
	if i := strings.IndexByte(s, ':'); i < 0 {
		name = s
	} else {
		name, id = s[:i], s[i+1:]
	}
	return
}

// ResolverBlacklist implements a negative caching for Resolver instances.
// It works by registering addresses that should be filtered out of a service
// name resolution result, with a deadline at which the address blacklist will
// expire.
type ResolverBlacklist struct {
	mutex    sync.RWMutex
	version  uint64
	cleaning uint64
	addrs    map[string]time.Time
}

const (
	resolverBlacklistCleanupInterval = 1000
)

// Blacklist adds a blacklisted address, which expires and expireAt is reached.
func (blacklist *ResolverBlacklist) Blacklist(addr string, expireAt time.Time) {
	blacklist.mutex.Lock()
	if blacklist.addrs == nil {
		blacklist.addrs = make(map[string]time.Time)
	}
	blacklist.addrs[addr] = expireAt
	blacklist.mutex.Unlock()
}

// Filter takes a slice of endpoints and the current time, and returns that
// same slice trimmed, where all blacklisted addresses have been filtered out.
func (blacklist *ResolverBlacklist) Filter(endpoints []Endpoint, now time.Time) []Endpoint {
	version := atomic.AddUint64(&blacklist.version, 1)

	blacklist.mutex.RLock()
	blackListLength := len(blacklist.addrs)
	endpointsLength := 0

	if blackListLength == 0 {
		// Fast path, most of the time the black list is expected to be empty,
		// no need to pay the price of going through the endpoints in this case.
		endpointsLength = len(endpoints)
	} else {
		for i := range endpoints {
			expireAt, blacklisted := blacklist.addrs[endpoints[i].Addr.String()]

			if !blacklisted || now.After(expireAt) {
				endpoints[endpointsLength] = endpoints[i]
				endpointsLength++
			}
		}
	}

	blacklist.mutex.RUnlock()

	if blackListLength != 0 && (version%resolverBlacklistCleanupInterval) == 0 {
		if atomic.CompareAndSwapUint64(&blacklist.cleaning, 0, 1) {
			blacklist.cleanup(now)
			atomic.StoreUint64(&blacklist.cleaning, 0)
		}
	}

	return endpoints[:endpointsLength]
}

func (blacklist *ResolverBlacklist) cleanup(now time.Time) {
	blacklist.mutex.RLock()

	for addr, expireAt := range blacklist.addrs {
		if now.After(expireAt) {
			blacklist.mutex.RUnlock()
			blacklist.mutex.Lock()

			if expireAt := blacklist.addrs[addr]; now.After(expireAt) {
				delete(blacklist.addrs, addr)
			}

			blacklist.mutex.Unlock()
			blacklist.mutex.RLock()
		}
	}

	blacklist.mutex.RUnlock()
}
