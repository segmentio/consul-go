package consul

import (
	"context"
	"io/ioutil"
	"net/http"
	"testing"
	"time"
)

func TestListener(t *testing.T) {
	httpLstn, err := (&Listener{
		ServiceName:                         "test-listener",
		CheckHTTP:                           "/",
		CheckInterval:                       10 * time.Second,
		CheckDeregisterCriticalServiceAfter: 90 * time.Second,
	}).ListenContext(context.Background(), "tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	defer httpLstn.Close()

	httpServer := &http.Server{
		Handler: http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
			res.Write([]byte("Hello World!"))
		}),
	}
	go httpServer.Serve(httpLstn)
	defer httpServer.Close()

	// The HTTP client uses a transport with a resolver that uses consul to
	// lookup service addresses.
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: (&Dialer{}).DialContext,
		},
	}

	res, err := httpClient.Get("http://test-listener/")
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
