package consul

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestDialer(t *testing.T) {
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
		json.NewEncoder(res).Encode([]struct{ Service service }{{Service: service{addr, port}}})
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

	res, err := httpClient.Get("http://whaetever:0/")
	if err != nil {
		t.Error(err)
	}
	b, err := ioutil.ReadAll(res.Body)
	res.Body.Close()

	if s := string(b); s != "Hello World!" {
		t.Error("bad response:", s)
	}
}
