package httpconsul

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	consul "github.com/segmentio/consul-go"
)

func TestResolver(t *testing.T) {
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
