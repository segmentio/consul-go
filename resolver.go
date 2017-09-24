package consul

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
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

	// The balancer is used to reorder the list of endpoints returned by the
	// resolver when looking up services.
	//
	// This field takes precedence over Sort, to use a simple sorting function,
	// set the value to nil.
	Balancer Balancer

	// Sort is called to order the list of endpoints returned by the resolver.
	// Setting this field to nil means no ordering of the endpoints is done.
	//
	// If the resolver is intended to be used to distribute load across a pool
	// of services it is important to set a Sort function that shuffles the list
	// of endpoints, otherwise consecutive calls would likely return the list in
	// the same order, and picking the first item would result in routing all
	// traffic to a single instance of the service.
	//
	// DEPRECATED: use Balancer instead.
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

	if cache := rslv.Cache; cache != nil {
		list, err = cache.LookupService(ctx, name, rslv.lookupService)
	} else {
		list, err = rslv.lookupService(ctx, name)
	}

	if err != nil {
		return nil, err
	}

	if rslv.Blacklist != nil {
		list = rslv.Blacklist.Filter(list, time.Now())
	}

	if rslv.Balancer != nil {
		list = rslv.Balancer.Balance(name, list)
	} else if rslv.Sort != nil {
		rslv.Sort(list)
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

	serviceName, serviceID := splitNameID(name)

	if err = rslv.client().Get(ctx, "/v1/health/service/"+serviceName, query, &results); err != nil {
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
	Cache:       &ResolverCache{Balancer: defaultCacheBalancer()},
	Blacklist:   &ResolverBlacklist{},
	Balancer:    &LoadBalancer{New: func() Balancer { return &RoundRobin{} }},
	Sort:        WeightedShuffleOnRTT,
}

// LookupHost is a wrapper around the default resolver's LookupHost
// method.
func LookupHost(ctx context.Context, name string) ([]string, error) {
	return DefaultResolver.LookupHost(ctx, name)
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

	// A balancer used by the cache to potentially filter or reorder endpoints
	// from the resolved names before caching them.
	Balancer Balancer

	// Pointer to *resolverCache where cached service endpoints are read from.
	// The field is manipulated using atomic operations to prevent cache
	// updates from ever blocking service lookups.
	cmap unsafe.Pointer

	// Version of the resolver, incremented every time it gets updated.
	version uint64

	// This map keeps track of all in-flight resolution to avoid making more
	// than one concurrent request to the actual resolver.
	mutex    sync.Mutex
	inflight map[string](chan struct{})
}

const (
	resolverCacheCleanupInterval = 1000
)

// LookupService resolves a service name by fetching the address list from the
// cache, or calling lookup if the name did not exist.
func (cache *ResolverCache) LookupService(ctx context.Context, name string, lookup LookupServiceFunc) ([]Endpoint, error) {
	cacheTimeout := cache.cacheTimeout()
	entry := cache.cache()[name]
	now := time.Now()

	for entry == nil || now.After(entry.expireAt) {
		var err error
		// Slow path: when the entry doesn't exist or was expired the goroutines
		// that concurrently attempt to resolve the same service name will sync
		// on the inflight map to make a single call to the lookup function and
		// udpate the cache.
		if entry, err = cache.lookup(ctx, name, lookup); err != nil {
			return nil, err
		}
	}

	// To reduce the chances of getting cache misses on expired entries we
	// prefetch the updated list of addresses when we're getting close to the
	// expiration time. This is not a perfect solution and works when fetching
	// the address list completes before the cleanup goroutine gets rid of the
	// cache entry, but it has the advantage of being a fully non-blocking
	// approach.
	if entry.expireAt.Sub(now) <= (cacheTimeout / 10) {
		// Attempt to acquire the entry's lock so only a single goroutine
		// prefetches the new value. The entry is not unlocked on purpose so
		// we don't endup hammering the backend if an error occurs.
		if entry.tryLock() {
			// Only proactively update the cache entry if there was no error.
			if res, err := lookup(ctx, name); err != nil {
				cache.update(name, &resolverEntry{
					res:      res,
					err:      err,
					expireAt: time.Now().Add(cacheTimeout),
				})
			}
		}
	}

	// We have to make a copy to let the caller own the value returned by this
	// method. Otherwise it could make changes that modify the cache's internal
	// memory, which would cause races and unexpected behaviors between calls to
	// the LookupService method.
	list := make([]Endpoint, len(entry.res))
	copy(list, entry.res)
	return list, entry.err
}

func (cache *ResolverCache) cacheTimeout() time.Duration {
	if cacheTimeout := cache.CacheTimeout; cacheTimeout != 0 {
		return cache.CacheTimeout
	}
	return 1 * time.Second
}

func (cache *ResolverCache) cache() resolverCache {
	cmap := cache.load()
	if cmap == nil {
		return nil
	}
	return *cmap
}

func (cache *ResolverCache) load() *resolverCache {
	return (*resolverCache)(atomic.LoadPointer(&cache.cmap))
}

func (cache *ResolverCache) compareAndSwap(old *resolverCache, new *resolverCache) bool {
	return atomic.CompareAndSwapPointer(&cache.cmap, unsafe.Pointer(old), unsafe.Pointer(new))
}

func (cache *ResolverCache) update(name string, entry *resolverEntry) {
	for {
		oldCache := cache.load()
		newCache := oldCache.copy()
		newCache[name] = entry

		if cache.compareAndSwap(oldCache, &newCache) {
			break
		}
	}

	if (atomic.AddUint64(&cache.version, 1) % resolverCacheCleanupInterval) == 0 {
		cache.cleanup()
	}
}

func (cache *ResolverCache) cleanup() {
	for {
		oldCache := cache.load()
		newCache := oldCache.copy()
		now := time.Now()

		for name, entry := range newCache {
			if now.After(entry.expireAt) {
				delete(newCache, name)
			}
		}

		if cache.compareAndSwap(oldCache, &newCache) {
			break
		}
	}
}

func (cache *ResolverCache) lookup(ctx context.Context, name string, lookup LookupServiceFunc) (*resolverEntry, error) {
	cache.mutex.Lock()
	ch, ok := cache.inflight[name]
	if !ok {
		if cache.inflight == nil {
			cache.inflight = make(map[string](chan struct{}))
		}
		ch = make(chan struct{})
		cache.inflight[name] = ch
	}
	cache.mutex.Unlock()

	if ok {
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return cache.cache()[name], nil
	}

	defer func() {
		cache.mutex.Lock()
		close(ch)
		delete(cache.inflight, name)
		cache.mutex.Unlock()
	}()

	res, err := lookup(ctx, name)

	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	entry := &resolverEntry{
		res:      res,
		err:      err,
		expireAt: time.Now().Add(cache.cacheTimeout()),
	}
	cache.update(name, entry)
	return entry, nil
}

type resolverCache map[string]*resolverEntry

func (cache *resolverCache) add(name string, entry *resolverEntry) *resolverCache {
	copy := cache.copy()
	copy[name] = entry
	return &copy
}

func (cache *resolverCache) copy() resolverCache {
	copy := make(resolverCache)
	if cache != nil {
		for k, v := range *cache {
			copy[k] = v
		}
	}
	return copy
}

type resolverEntry struct {
	// Immutable fields, cache entries are replaced when they have to change.
	res      []Endpoint
	err      error
	expireAt time.Time

	// Lock used to ensure that only a single goroutine takes care of refreshing
	// the cache entry before it expires.
	lock uint32
}

func (entry *resolverEntry) tryLock() bool {
	return atomic.CompareAndSwapUint32(&entry.lock, 0, 1)
}

func (entry *resolverEntry) unlock() {
	atomic.StoreUint32(&entry.lock, 0)
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
	length   int64          // number of items in the map
	version  uint32         // counter for cleaning up the resolver map
	cleaning uint32         // lock to ensure only one goroutine cleans the map
	addrs    unsafe.Pointer // *blacklistCache
}

const (
	resolverBlacklistCleanupInterval = 1000
)

// Blacklist adds a blacklisted address, which expires and expireAt is reached.
func (blacklist *ResolverBlacklist) Blacklist(addr net.Addr, expireAt time.Time) {
	atomic.AddInt64(&blacklist.length, 1)

	for {
		oldCache := blacklist.loadCache()
		newCache := oldCache.copy()
		newCache[addr.String()] = expireAt

		if blacklist.compareAndSwapCache(oldCache, &newCache) {
			break
		}
	}
}

// Filter takes a slice of endpoints and the current time, and returns that
// same slice trimmed, where all blacklisted addresses have been filtered out.
func (blacklist *ResolverBlacklist) Filter(endpoints []Endpoint, now time.Time) []Endpoint {
	// In the common case where there is no endpoints in the blacklist, the
	// code takes this fast non-blocking path.
	if atomic.LoadInt64(&blacklist.length) == 0 {
		return endpoints
	}

	cache := blacklist.cache()
	endpointsLength := 0

	for i := range endpoints {
		expireAt, blacklisted := cache[endpoints[i].Addr.String()]

		if !blacklisted || now.After(expireAt) {
			endpoints[endpointsLength] = endpoints[i]
			endpointsLength++
		}
	}

	if version := atomic.AddUint32(&blacklist.version, 1); (version % resolverBlacklistCleanupInterval) == 0 {
		if atomic.CompareAndSwapUint32(&blacklist.cleaning, 0, 1) {
			blacklist.cleanup(now)
			atomic.StoreUint32(&blacklist.cleaning, 0)
		}
	}

	return endpoints[:endpointsLength]
}

func (blacklist *ResolverBlacklist) cleanup(now time.Time) {
	for {
		deleted := int64(0)
		oldCache := blacklist.loadCache()
		newCache := oldCache.copy()

		for addr, expireAt := range *oldCache {
			if now.After(expireAt) {
				delete(newCache, addr)
				deleted++
			}
		}

		if blacklist.compareAndSwapCache(oldCache, &newCache) {
			atomic.AddInt64(&blacklist.length, -deleted)
			break
		}
	}
}

func (blacklist *ResolverBlacklist) cache() blacklistCache {
	cache := blacklist.loadCache()
	if cache == nil {
		return nil
	}
	return *cache
}

func (blacklist *ResolverBlacklist) loadCache() *blacklistCache {
	return (*blacklistCache)(atomic.LoadPointer(&blacklist.addrs))
}

func (blacklist *ResolverBlacklist) compareAndSwapCache(old *blacklistCache, new *blacklistCache) bool {
	return atomic.CompareAndSwapPointer(&blacklist.addrs, unsafe.Pointer(old), unsafe.Pointer(new))
}

type blacklistCache map[string]time.Time

func (m *blacklistCache) copy() blacklistCache {
	c := make(blacklistCache)
	if m != nil {
		for k, v := range *m {
			c[k] = v
		}
	}
	return c
}
