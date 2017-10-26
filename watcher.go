package consul

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"time"
)

// WatcherFunc is the function signature for the callback from watch.  It
// passes a list of KeyData representing the most recent value or values stored
// at the key or everything below the prefix. It can be nil.  error is passed
// through to the handler if it is not Temporary. Caller is responsible for
// stopping the watch via canceling the context and implementing back off
// logic.  If the error is temporary, it will not pass it through to the
// WatcherFunc unless MaxAttempts was hit.
type WatcherFunc func([]KeyData, error)

// Watcher is the struct upon which Watch and WatchPrefix are built.
type Watcher struct {
	Client *Client
	// MaxAttempts limits the number of subsequent failed API calls before
	// bailing out of the watch.
	MaxAttempts int
}

var (
	DefaultWatcher = &Watcher{}

	// WatchTransport is the same as DefaultTransport with a longer
	// ResponseHeaderTimeout.  This is copied from DefaultTransport.  We don't
	// do an actual copy because the Transport uses mutexes.  copied mutexes
	// are ineffective.  You will panic on concurrent map access if running
	// with other clients without instantiating a new transport for watch.
	WatchTransport http.RoundTripper = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 5 * time.Second,
		}).DialContext,
		DisableCompression:    true,
		MaxIdleConns:          5,
		MaxIdleConnsPerHost:   2,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 1 * time.Minute,
		ExpectContinueTimeout: 5 * time.Second,
	}
)

func (w *Watcher) client() *Client {
	if client := w.Client; client != nil {
		return client
	}
	c := &Client{
		Transport: WatchTransport,
	}
	return c
}

// Watch is the package-level Watch definition which is called on
// DefaultWatcher.
func Watch(ctx context.Context, key string, handler WatcherFunc) {
	DefaultWatcher.Watch(ctx, key, handler)
}

// WatchPrefix is the package-level WatchPrefix definition which is called on
// DefaultWatcher.
func WatchPrefix(ctx context.Context, prefix string, handler WatcherFunc) {
	DefaultWatcher.WatchPrefix(ctx, prefix, handler)
}

// Watch executes a long poll for changes to the given key.  handler will be
// called immediately upon registration to initialize the watch and returns
// the initial value (as a list, this is what Consul API returns).  There is
// a chance that Watch can miss updates due to the way the API is implemented.
// See https://github.com/hashicorp/consul/issues/1761 for details.
func (w *Watcher) Watch(ctx context.Context, key string, handler WatcherFunc) {
	w.watching(ctx, key, handler, nil)
}

// WatchPrefix executes a long poll for changes to anything under the given
// prefix.  handler will be called immediately upon registration to initialize
// the watch and returns the initial value (as a list, this is what Consul API
// returns).  There is a chance that WatchPrefix can miss updates due to the
// way the API is implemented.  See https://github.com/hashicorp/consul/issues/1761
// for details.
func (w *Watcher) WatchPrefix(ctx context.Context, prefix string, handler WatcherFunc) {
	q := Query{Param{
		Name:  "recurse",
		Value: "",
	}}
	w.watching(ctx, prefix, handler, q)
}

func (w *Watcher) watching(ctx context.Context, key string, handler WatcherFunc, q Query) {
	if w.MaxAttempts <= 0 {
		w.MaxAttempts = 10
	}

	path := "/v1/kv/" + key
	value := "0"

	attempt := 0
	for {
		q.Add(Param{Name: "index", Value: value})
		resp := []KeyData{}
		hdr, err := w.client().do(ctx, "GET", path, q, nil, &resp)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			attempt += 1
			if attempt >= w.MaxAttempts {
				handler(nil, err)
				continue
			}
			if v, ok := err.(interface {
				Temporary() bool
			}); ok {
				if v.Temporary() {
					continue
				}
			}
		} else {
			attempt = 0
		}
		handler(resp, err)
		value = strconv.FormatUint(hdr.index, 10)
	}
}
