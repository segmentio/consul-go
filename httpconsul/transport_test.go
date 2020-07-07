package httpconsul

import (
	"encoding/json"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	consul "github.com/segmentio/consul-go"
)

func TestTransport(t *testing.T) {
	t.Run("sending requests to existing services results in a getting a response from one of the endpoints",
		testTransportRequestExistingService)

	t.Run("sending requests to non-existing services results in denylisting the endpoints and an error after a couple of attempts",
		testTransportRequestNonExistingService)
}

func testTransportRequestExistingService(t *testing.T) {
	httpServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.Write([]byte("Hello World!"))
	}))
	defer httpServer.Close()
	u, _ := url.Parse(httpServer.URL)
	addr, port := splitHostPort(u.Host)

	consulServer, consulClient := newServerClient(func(res http.ResponseWriter, req *http.Request) {
		type service struct {
			Address string
			Port    int
		}
		json.NewEncoder(res).Encode([]struct{ Service service }{{Service: service{addr, atoi(port)}}})
	})
	defer consulServer.Close()

	// The HTTP client uses a transport with a resolver that uses consul to
	// lookup service addresses.
	httpClient := &http.Client{
		Transport: NewTransportWith(&http.Transport{}, &consul.Resolver{
			Client: consulClient,
		}),
	}

	res, err := httpClient.Get("http://whatever/")
	if err != nil {
		t.Error(err)
		return
	}
	b, _ := ioutil.ReadAll(res.Body)
	res.Body.Close()

	if s := string(b); s != "Hello World!" {
		t.Error("bad response:", s)
	}
}

func testTransportRequestNonExistingService(t *testing.T) {
	consulServer, consulClient := newServerClient(func(res http.ResponseWriter, req *http.Request) {
		type service struct {
			Address string
			Port    int
		}
		json.NewEncoder(res).Encode([]struct{ Service service }{{Service: service{"192.0.2.0", 42}}})
	})
	defer consulServer.Close()

	// The HTTP client uses a transport with a resolver that uses consul to
	// lookup service addresses.
	httpClient := &http.Client{
		Transport: NewTransportWith(
			&http.Transport{
				DialContext: (&net.Dialer{
					Timeout: 10 * time.Millisecond,
				}).DialContext,
			},
			&consul.Resolver{
				Client:   consulClient,
				Denylist: &consul.ResolverDenylist{},
			},
		),
	}

	res, err := httpClient.Get("http://whatever/")
	if err == nil {
		res.Body.Close()
		t.Error("no error returned when sending a request to an invalid address")
	}
}

func atoi(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

func newServerClient(handler func(http.ResponseWriter, *http.Request)) (server *httptest.Server, client *consul.Client) {
	server = httptest.NewServer(http.HandlerFunc(handler))
	client = &consul.Client{
		Address:    server.URL,
		UserAgent:  "test",
		Datacenter: "dc1",
	}
	return
}
