package consul

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	"github.com/segmentio/objconv/json"
)

func TestResolver(t *testing.T) {
	t.Run("LookupService", testLookupService)
}

func testLookupService(t *testing.T) {
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
			"near":      {"_agent"},
			"tag":       {"A", "B", "C", "a", "b", "c"},
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
		Near:        "_agent",
		ServiceTags: []string{"A", "B", "C"},
		NodeMeta:    map[string]string{"answer": "42"},
	}

	addrs, err := rslv.LookupService(context.Background(), "1234", "a", "b", "c")

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
