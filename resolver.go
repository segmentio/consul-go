package consul

import (
	"context"
	"net"
	"runtime"
	"strconv"
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

	// Cache used by the resolver to reduce the number of round-trips to consul.
	// If set to nil then no cache is used.
	//
	// ResolverCache instances should not be shared by multiple resolvers
	// because the cache uses the service name as a lookup key, but the resolver
	// may apply filters based on its configuration.
	Cache *ResolverCache
}

// LookupService resolves a service name to a list of addresses using the
// resolver's configuration to narrow the result set. Only addresses of healthy
// services are returned by the lookup operation.
func (rslv *Resolver) LookupService(ctx context.Context, name string) ([]net.Addr, error) {
	if rslv.Cache != nil {
		return rslv.Cache.LookupService(ctx, name, rslv.lookupService)
	}
	return rslv.lookupService(ctx, name)
}

func (rslv *Resolver) lookupService(ctx context.Context, name string) (addrs []net.Addr, err error) {
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

	query := make(Query, 0, 2+len(rslv.NodeMeta)+len(rslv.ServiceTags))
	query = append(query, Param{Name: "passing"})

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

	if near := rslv.Near; len(near) != 0 {
		query = append(query, Param{
			Name:  "near",
			Value: near,
		})
	}

	if err = rslv.client().Get(ctx, "/v1/health/service/"+name, query, &results); err != nil {
		return
	}

	addrs = make([]net.Addr, len(results))

	for i, res := range results {
		addrs[i] = &serviceAddr{
			addr: res.Service.Address,
			port: res.Service.Port,
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

// DefaultResolver is the Resolver used by a Dialer when non has been specified.
var DefaultResolver = &Resolver{
	Near: "_agent",
}

// LookupService is a wrapper around the default resolver's LookupService
// method.
func LookupService(ctx context.Context, name string) ([]net.Addr, error) {
	return DefaultResolver.LookupService(ctx, name)
}

type serviceAddr struct {
	addr string
	port int
}

func (a *serviceAddr) Network() string {
	return ""
}

func (a *serviceAddr) String() string {
	return net.JoinHostPort(a.addr, strconv.Itoa(a.port))
}

// LookupServiceFunc is the signature of functions that can be used to lookup
// service names.
type LookupServiceFunc func(context.Context, string) ([]net.Addr, error)

// The ResolverCache type provides the implementation of a caching layer for
// service name resolutions.
//
// Instances of ResolverCache are save to use concurrently from multiple
// goroutines.
type ResolverCache struct {
	// The maximum age of cache entries. If zero, a default value of 1 second is
	// used.
	Timeout time.Duration

	once sync.Once
	cmap *resolverCacheMap
}

// LookupService resolves a service name by fetching the address list from the
// cache, or calling lookup if the name did not exist.
func (cache *ResolverCache) LookupService(ctx context.Context, name string, lookup LookupServiceFunc) ([]net.Addr, error) {
	cache.once.Do(cache.init)

	now := time.Now()
	timeout := cache.timeout()
	entry, locked := cache.cmap.lookup(name, now, now.Add(timeout))

	if locked {
		// TODO: check the error type here and discard things like context
		// cancellations and timeouts?
		entry.addrs, entry.err = lookup(ctx, name)
		entry.mutex.Unlock()
	}

	entry.mutex.RLock()
	addrs, err, expireAt := entry.addrs, entry.err, entry.expireAt
	entry.mutex.RUnlock()

	// To reduce the chances of getting cache misses on expired entries we
	// prefetch the updated list of addresses when we're getting close to the
	// expiration time. This is not a perfect solution and works when fetching
	// the address list completes before the cleanup goroutine gets rid of the
	// cache entry, but it has the advantage of being a fully non-blocking
	// approach.
	if now.After(expireAt.Add(-timeout / 10)) {
		if atomic.CompareAndSwapUint32(&entry.lock, 0, 1) {
			addrs, err := lookup(ctx, name)
			exp := time.Now().Add(timeout)

			entry.mutex.Lock()
			entry.addrs = addrs
			entry.err = err
			entry.expireAt = exp
			entry.mutex.Unlock()

			atomic.StoreUint32(&entry.lock, 0)
		}
	}

	return addrs, err
}

func (cache *ResolverCache) init() {
	cache.cmap = &resolverCacheMap{
		entries: make(map[string]*resolverCacheEntry),
	}

	ctx, cancel := context.WithCancel(context.Background())
	runtime.SetFinalizer(cache, func(_ *ResolverCache) { cancel() })

	interval := cache.timeout() / 2
	go cache.cmap.autoDeleteExpired(ctx, interval)
}

func (cache *ResolverCache) timeout() time.Duration {
	if timeout := cache.Timeout; timeout != 0 {
		return cache.Timeout
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
	addrs    []net.Addr
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
