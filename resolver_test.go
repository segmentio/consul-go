package consul

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/objconv/json"
)

func TestResolver(t *testing.T) {
	t.Run("LookupService", func(t *testing.T) {
		t.Run("uncached", func(t *testing.T) { testLookupService(t, nil) })
		t.Run("cached", func(t *testing.T) { testLookupService(t, &ResolverCache{}) })
	})
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
			"passing":   {""},
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

	addrs, err := rslv.LookupService(context.Background(), "1234")

	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(addrs, []Endpoint{
		{Addr: &serviceAddr{"192.168.0.1", 4242}},
		{Addr: &serviceAddr{"192.168.0.2", 4242}},
		{Addr: &serviceAddr{"192.168.0.3", 4242}},
	}) {
		t.Error("bad addresses returned:", addrs)
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
			"passing":   {""},
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

func TestResolverCache(t *testing.T) {
	list := []Endpoint{
		{Addr: &serviceAddr{"192.168.0.1", 4242}},
		{Addr: &serviceAddr{"192.168.0.2", 4242}},
		{Addr: &serviceAddr{"192.168.0.3", 4242}},
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

		// Loop for 30ms, there should 4 cache misses due to prefetching at
		// 0ms, 9ms, 19ms, and 29ms.
		for now, exp := time.Now(), time.Now().Add(30*time.Millisecond); now.Before(exp); now = time.Now() {
			addrs, err := cache.LookupService(context.Background(), "", lookup)

			if err != nil {
				t.Error("error returned by service lookup:", err)
			}

			if !reflect.DeepEqual(addrs, list) {
				t.Error("bad address list returned by service lookup:", err)
			}
		}

		if n := atomic.LoadInt32(&miss); n != 4 {
			t.Error("bad number of cache misses:", n)
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
