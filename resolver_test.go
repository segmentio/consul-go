package consul

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestResolver(t *testing.T) {
	t.Run("LookupService", func(t *testing.T) {
		t.Run("uncached", func(t *testing.T) { testLookupService(t, nil) })
		t.Run("cached", func(t *testing.T) { testLookupService(t, &ResolverCache{}) })
		t.Run("by-name", func(t *testing.T) { testLookupServiceByName(t, nil) })
		t.Run("by-ID", func(t *testing.T) { testLookupServiceByID(t, nil) })
		t.Run("looking up service by names or IDs works properly with cache and balancers", testLookupServiceWithBalancer)
		t.Run("looking up service by name into slice returns re-slices to proper len", func(t *testing.T) { testLookupServiceInto(t) })
	})
	t.Run("LookupHost", func(t *testing.T) {
		t.Run("uncached", func(t *testing.T) { testLookupHost(t, nil) })
		t.Run("cached", func(t *testing.T) { testLookupService(t, &ResolverCache{}) })
	})
}

func testLookupServiceInto(t *testing.T) {
	t.Parallel()
	dirtyEndpoints := []Endpoint{
		{Addr: newServiceAddr("192.168.0.1", 4242)},
		{Addr: newServiceAddr("192.168.0.2", 4242)},
		{Addr: newServiceAddr("192.168.0.3", 4242)},
	}
	newEndpoints := []Endpoint{{Addr: newServiceAddr("192.168.0.10", 4242)}}
	cache := &ResolverCache{}
	newReturnedEp, _ := cache.LookupServiceInto(context.Background(), "", dirtyEndpoints, func(ctx context.Context, name string) (addrs []Endpoint, err error) {
		return newEndpoints, nil
	})

	if len(newReturnedEp) != len(newEndpoints) || !reflect.DeepEqual(newReturnedEp[0], newEndpoints[0]) {
		t.Error("LookupServiceInto returned unclean slice")
	}
}

func testLookupService(t *testing.T, cache *ResolverCache) {
	server, client := newServerClient(func(res http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			t.Error("bad method:", req.Method)
		}

		if req.URL.Path != "/v1/health/service/1234" {
			t.Error("bad URL path:", req.URL.Path)
		}

		foundQuery := req.URL.Query()
		expectQuery := url.Values{
			"passing":   {"true"},
			"stale":     {"true"},
			"cached":    {"true"},
			"dc":        {"dc1"},
			"tag":       {"A", "B", "C"},
			"node-meta": {"answer:42"},
		}
		if !reflect.DeepEqual(foundQuery, expectQuery) {
			t.Error("bad URL query:")
			t.Logf("expected: %#v", expectQuery)
			t.Logf("found:    %#v", foundQuery)
		}

		type service struct {
			Address string
			Port    int
		}
		json.NewEncoder(res).Encode([]struct {
			Service service
		}{
			{Service: service{Address: "192.168.0.1", Port: 4242}},
			{Service: service{Address: "192.168.0.2", Port: 4242}},
			{Service: service{Address: "192.168.0.3", Port: 4242}},
		})
		return
	})
	defer server.Close()

	rslv := Resolver{
		Client:      client,
		ServiceTags: []string{"A", "B", "C"},
		NodeMeta:    map[string]string{"answer": "42"},
		OnlyPassing: true,
		AllowStale:  true,
		AllowCached: true,
		Cache:       cache,
	}

	addrs, err := rslv.LookupService(context.Background(), "1234")

	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(addrs, []Endpoint{
		{Addr: newServiceAddr("192.168.0.1", 4242)},
		{Addr: newServiceAddr("192.168.0.2", 4242)},
		{Addr: newServiceAddr("192.168.0.3", 4242)},
	}) {
		t.Error("bad addresses returned:", addrs)
	}
}

func testLookupServiceByName(t *testing.T, cache *ResolverCache) {
	lstn := &Listener{
		ServiceName: "test-consul-go-by-name",
		ServiceID:   "1234",
	}

	l, err := lstn.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	rslv := Resolver{
		OnlyPassing: true,
		Cache:       cache,
	}

	addrs, err := rslv.LookupService(context.Background(), "test-consul-go-by-name")
	if err != nil {
		t.Fatal(err)
	}
	if len(addrs) != 1 {
		t.Fatal("bad address count:", addrs)
	}
	if addrs[0].ID != "test-consul-go-by-name:1234" {
		t.Error("bad ID:", addrs[0].ID)
	}
	if addrs[0].Addr.String() != l.Addr().String() {
		t.Error("bad address:", addrs[0].Addr)
	}
}

func testLookupServiceByID(t *testing.T, cache *ResolverCache) {
	lstn := &Listener{
		ServiceName: "test-consul-go-by-ID",
		ServiceID:   "1234",
	}

	l, err := lstn.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	rslv := Resolver{
		OnlyPassing: true,
		Cache:       cache,
	}

	addrs, err := rslv.LookupService(context.Background(), "test-consul-go-by-ID:1234")
	if err != nil {
		t.Fatal(err)
	}
	if len(addrs) != 1 {
		t.Fatal("bad address count:", addrs)
	}
	if addrs[0].ID != "test-consul-go-by-ID:1234" {
		t.Error("bad ID:", addrs[0].ID)
	}
	if addrs[0].Addr.String() != l.Addr().String() {
		t.Error("bad address:", addrs[0].Addr)
	}
}

func testLookupHost(t *testing.T, cache *ResolverCache) {
	server, client := newServerClient(func(res http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			t.Error("bad method:", req.Method)
		}

		if req.URL.Path != "/v1/health/service/1234" {
			t.Error("bad URL path:", req.URL.Path)
		}

		foundQuery := req.URL.Query()
		expectQuery := url.Values{
			"passing":   {"true"},
			"dc":        {"dc1"},
			"tag":       {"A", "B", "C"},
			"node-meta": {"answer:42"},
		}
		if !reflect.DeepEqual(foundQuery, expectQuery) {
			t.Error("bad URL query:")
			t.Logf("expected: %#v", expectQuery)
			t.Logf("found:    %#v", foundQuery)
		}

		type service struct {
			Address string
			Port    int
		}
		json.NewEncoder(res).Encode([]struct {
			Service service
		}{
			{Service: service{Address: "192.168.0.1", Port: 4242}},
			{Service: service{Address: "192.168.0.2", Port: 4242}},
			{Service: service{Address: "192.168.0.3", Port: 4242}},
		})
		return
	})
	defer server.Close()

	rslv := Resolver{
		Client:      client,
		ServiceTags: []string{"A", "B", "C"},
		NodeMeta:    map[string]string{"answer": "42"},
		OnlyPassing: true,
		Cache:       cache,
	}

	addrs, err := rslv.LookupHost(context.Background(), "1234")

	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(addrs, []string{
		"192.168.0.1:4242",
		"192.168.0.2:4242",
		"192.168.0.3:4242",
	}) {
		t.Error("bad addresses returned:", addrs)
	}
}

func testLookupServiceWithBalancer(t *testing.T) {
	lstn := &Listener{
		ServiceName: "test-consul-go-with-balancer",
		ServiceID:   "1234",
		ServiceTags: []string{"us-west-2b"},
	}

	l1, err := lstn.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l1.Close()

	lstn.ServiceID = "5678"
	lstn.ServiceTags = []string{"us-west-2a"}

	l2, err := lstn.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()

	rslv := Resolver{
		OnlyPassing: true,
		Balancer:    &RoundRobin{},
		Cache:       &ResolverCache{Balancer: PreferTags{"us-west-2a"}},
	}

	for i := 0; i != 4; i++ {
		addrs, err := rslv.LookupService(context.Background(), "test-consul-go-with-balancer")
		if err != nil {
			t.Fatal(err)
		}
		if len(addrs) != 1 {
			t.Fatal("bad address count:", addrs)
		}
		if addrs[0].ID != "test-consul-go-with-balancer:5678" {
			t.Error("bad ID:", addrs[0].ID)
		}
		if addrs[0].Addr.String() != l2.Addr().String() {
			t.Error("bad address:", addrs[0].Addr)
		}
	}

	for i := 0; i != 4; i++ {
		addrs, err := rslv.LookupService(context.Background(), "test-consul-go-with-balancer:1234")
		if err != nil {
			t.Fatal(err)
		}
		if len(addrs) != 1 {
			t.Fatal("bad address count:", addrs)
		}
		if addrs[0].ID != "test-consul-go-with-balancer:1234" {
			t.Error("bad ID:", addrs[0].ID)
		}
		if addrs[0].Addr.String() != l1.Addr().String() {
			t.Error("bad address:", addrs[0].Addr)
		}
	}
}

func TestResolverCache(t *testing.T) {
	list := []Endpoint{
		{Addr: newServiceAddr("192.168.0.1", 4242)},
		{Addr: newServiceAddr("192.168.0.2", 4242)},
		{Addr: newServiceAddr("192.168.0.3", 4242)},
	}

	t.Run("ensure there are cache hits when making service lookup calls in a tight loop", func(t *testing.T) {
		t.Parallel()

		miss := int32(0)
		cache := &ResolverCache{
			CacheTimeout: 10 * time.Millisecond,
		}

		lookup := func(ctx context.Context, name string) (addrs []Endpoint, err error) {
			atomic.AddInt32(&miss, 1)
			return list, nil
		}

		i := 0

		for now, exp := time.Now(), time.Now().Add(30*time.Millisecond); now.Before(exp); now = time.Now() {
			addrs, err := cache.LookupService(context.Background(), "", lookup)

			if err != nil {
				t.Error("error returned by service lookup:", err)
			}

			if !reflect.DeepEqual(addrs, list) {
				t.Error("bad address list returned by service lookup:", err)
			}
			i++
		}

		if n := atomic.LoadInt32(&miss); n > 8 {
			t.Errorf("too many cache misses: %d/%d", n, i)
		}
	})

	t.Run("ensure the cache entries get expired when the service lookups are being done slowly", func(t *testing.T) {
		t.Parallel()

		miss := int32(0)
		cache := &ResolverCache{
			CacheTimeout: 10 * time.Millisecond,
		}

		lookup := func(ctx context.Context, name string) (addrs []Endpoint, err error) {
			atomic.AddInt32(&miss, 1)
			return list, nil
		}

		for i := 0; i != 4; i++ {
			addrs, err := cache.LookupService(context.Background(), "", lookup)

			if err != nil {
				t.Error("error returned by service lookup:", err)
			}

			if !reflect.DeepEqual(addrs, list) {
				t.Error("bad address list returned by service lookup:", addrs)
			}

			// sleep for a little while to let the cache entries expire
			time.Sleep(20 * time.Millisecond)
		}

		if n := atomic.LoadInt32(&miss); n != 4 {
			t.Error("bad number of cache misses:", n)
		}
	})
}

func BenchmarkResolverCache(b *testing.B) {
	list := []Endpoint{
		{Addr: newServiceAddr("192.168.0.1", 4242)},
		{Addr: newServiceAddr("192.168.0.2", 4242)},
		{Addr: newServiceAddr("192.168.0.3", 4242)},
	}

	cache := &ResolverCache{
		CacheTimeout: 10 * time.Millisecond,
	}

	lookup := func(ctx context.Context, name string) (addrs []Endpoint, err error) {
		return list, nil
	}

	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()

		for pb.Next() {
			cache.LookupService(ctx, "", lookup)
		}
	})
}

func BenchmarkResolverCacheInto(b *testing.B) {
	list := []Endpoint{
		{Addr: newServiceAddr("192.168.0.1", 4242)},
		{Addr: newServiceAddr("192.168.0.2", 4242)},
		{Addr: newServiceAddr("192.168.0.3", 4242)},
	}

	cache := &ResolverCache{
		CacheTimeout: 10 * time.Millisecond,
	}

	lookup := func(ctx context.Context, name string) (addrs []Endpoint, err error) {
		return list, nil
	}

	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()

		res := make([]Endpoint, 10)

		for pb.Next() {
			cache.LookupServiceInto(ctx, "", res, lookup)
		}
	})
}

func BenchmarkResolverCacheIntoResize(b *testing.B) {
	list := []Endpoint{
		{Addr: newServiceAddr("192.168.0.1", 4242)},
		{Addr: newServiceAddr("192.168.0.2", 4242)},
		{Addr: newServiceAddr("192.168.0.3", 4242)},
	}

	cache := &ResolverCache{
		CacheTimeout: 10 * time.Millisecond,
	}

	lookup := func(ctx context.Context, name string) (addrs []Endpoint, err error) {
		return list, nil
	}

	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()

		res := make([]Endpoint, 10)

		for pb.Next() {
			res = res[:0]
			cache.LookupServiceInto(ctx, "", res, lookup)
		}
	})
}

func TestResolverDenylist(t *testing.T) {
	tests := []struct {
		scenario string
		function func(*testing.T, *ResolverDenylist, []Endpoint)
	}{
		{
			scenario: "when no address is denylisted no address is filtered out",
			function: testResolverDenylistNoFilter,
		},
		{
			scenario: "denied addresses are filtered out of the endpoint list",
			function: testResolverDenylistFilter,
		},
		{
			scenario: "denied addresses are cleaned up after enough calls to Filter",
			function: testResolverDenylistCleanup,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			denylist := &ResolverDenylist{}
			endpoints := []Endpoint{
				{ID: "A", Addr: newServiceAddr("127.0.0.1", 1000)},
				{ID: "B", Addr: newServiceAddr("127.0.0.1", 1001)},
				{ID: "C", Addr: newServiceAddr("127.0.0.1", 1002)},
				{ID: "D", Addr: newServiceAddr("127.0.0.1", 1003)},
				{ID: "E", Addr: newServiceAddr("127.0.0.1", 1004)},
			}

			test.function(t, denylist, endpoints)
		})
	}
}

func testResolverDenylistNoFilter(t *testing.T, denylist *ResolverDenylist, endpoints []Endpoint) {
	now := time.Now()

	list := make([]Endpoint, len(endpoints))
	copy(list, endpoints)

	if unfiltered := denylist.Filter(list, now); !reflect.DeepEqual(unfiltered, endpoints) {
		t.Error("bad endpoint list:")
		t.Log("expected:", endpoints)
		t.Log("found:   ", unfiltered)
	}
}

func testResolverDenylistFilter(t *testing.T, denylist *ResolverDenylist, endpoints []Endpoint) {
	now := time.Now()

	denylist.Denylist(newServiceAddr("127.0.0.1", 1000), now.Add(time.Second))
	denylist.Denylist(newServiceAddr("127.0.0.1", 1003), now.Add(time.Millisecond))
	denylist.Denylist(newServiceAddr("192.168.0.1", 8080), now.Add(time.Hour))

	list := make([]Endpoint, len(endpoints))
	copy(list, endpoints)

	expected := []Endpoint{
		{ID: "B", Addr: newServiceAddr("127.0.0.1", 1001)},
		{ID: "C", Addr: newServiceAddr("127.0.0.1", 1002)},
		{ID: "D", Addr: newServiceAddr("127.0.0.1", 1003)},
		{ID: "E", Addr: newServiceAddr("127.0.0.1", 1004)},
	}

	if filtered := denylist.Filter(list, now.Add(500*time.Millisecond)); !reflect.DeepEqual(filtered, expected) {
		t.Error("bad endpoint list:")
		t.Log("expected:", expected)
		t.Log("found:   ", filtered)
	}
}

func testResolverDenylistCleanup(t *testing.T, denylist *ResolverDenylist, endpoints []Endpoint) {
	now := time.Now()

	denylist.Denylist(newServiceAddr("127.0.0.1", 1000), now.Add(time.Second))
	denylist.Denylist(newServiceAddr("127.0.0.1", 1003), now.Add(time.Millisecond))
	denylist.Denylist(newServiceAddr("192.168.0.1", 8080), now.Add(time.Hour))

	for i := 0; i != (resolverDenylistCleanupInterval + 1); i++ {
		list := make([]Endpoint, len(endpoints))
		copy(list, endpoints)
		denylist.Filter(list, now.Add(2*time.Second))
	}

	if !reflect.DeepEqual(denylist.cache(), denylistCache{
		"192.168.0.1:8080": now.Add(time.Hour),
	}) {
		t.Error("bad denylist state:", denylist.addrs)
	}
}

func BenchmarkResolverDenylist(b *testing.B) {
	b.Run("empty", func(b *testing.B) {
		now := time.Now()

		denylist := &ResolverDenylist{}

		b.RunParallel(func(pb *testing.PB) {
			endpoints := []Endpoint{
				{ID: "B", Addr: newServiceAddr("127.0.0.1", 1001)},
				{ID: "C", Addr: newServiceAddr("127.0.0.1", 1002)},
				{ID: "D", Addr: newServiceAddr("127.0.0.1", 1003)},
				{ID: "E", Addr: newServiceAddr("127.0.0.1", 1004)},
			}

			for pb.Next() {
				denylist.Filter(endpoints, now)
			}
		})
	})

	b.Run("filled", func(b *testing.B) {
		now := time.Now()
		exp := now.Add(time.Minute)

		denylist := &ResolverDenylist{}
		denylist.Denylist(newServiceAddr("127.0.0.1", 1002), exp)
		denylist.Denylist(newServiceAddr("127.0.0.1", 1003), exp)
		denylist.Denylist(newServiceAddr("127.0.0.1", 1005), exp)

		b.RunParallel(func(pb *testing.PB) {
			endpoints := []Endpoint{
				{ID: "B", Addr: newServiceAddr("127.0.0.1", 1001)},
				{ID: "C", Addr: newServiceAddr("127.0.0.1", 1002)},
				{ID: "D", Addr: newServiceAddr("127.0.0.1", 1003)},
				{ID: "E", Addr: newServiceAddr("127.0.0.1", 1004)},
			}

			for pb.Next() {
				denylist.Filter(endpoints, now)
			}
		})
	})
}
