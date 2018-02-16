package consul

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
)

func TestDialer(t *testing.T) {
	t.Run("dialing existing services results in a connection being established to one of the endpoints",
		testDialerDialExistingService)

	t.Run("dialing non-existing services results in blacklisting the endpoints and an error after a couple of attempts",
		testDialerDialNonExistingService)
}

func testDialerDialExistingService(t *testing.T) {
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
		Transport: &http.Transport{
			DialContext: (&Dialer{
				Resolver: &Resolver{
					Client: consulClient,
				},
			}).DialContext,
		},
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

func testDialerDialNonExistingService(t *testing.T) {
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
		Transport: &http.Transport{
			DialContext: (&Dialer{
				Timeout: 10 * time.Millisecond,
				Resolver: &Resolver{
					Client:    consulClient,
					Blacklist: &ResolverBlacklist{},
				},
			}).DialContext,
		},
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
