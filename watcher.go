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
// the initial value (as a list, this is what Consul API returns).  In cases
// where the key is being rapidly updated, there is a chance that Watch can
// miss intermediate updates due to the way the API is implemented.  However,
// Watch will always converge on the most recent value.  See
// https://github.com/hashicorp/consul/issues/1761 for details.
func (w *Watcher) Watch(ctx context.Context, key string, handler WatcherFunc) {
	w.watching(ctx, key, handler, nil)
}

// WatchPrefix executes a long poll for changes to anything under the given
// prefix.  handler will be called immediately upon registration to initialize
// the watch and returns the initial value (as a list, this is what Consul API
// returns).  In cases where children of prefix are being rapidly updated,
// there is a chance that WatchPrefix can miss intermediate updates due to the
// way the API is implemented.  However, WatchPrefix will always converge on
// the most recent values.  See
// https://github.com/hashicorp/consul/issues/1761 for details.
func (w *Watcher) WatchPrefix(ctx context.Context, prefix string, handler WatcherFunc) {
	q := Query{Param{Name: "recurse", Value: ""}}
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
		// bail if the client has cancelled the context
		if ctx.Err() != nil {
			return
		}
		q.Add(Param{Name: "index", Value: value})
		resp := []KeyData{}
		hdr, err := w.client().do(ctx, "GET", path, q, nil, &resp)
		if hdr.index > 0 {
			value = strconv.FormatUint(hdr.index, 10)
		}
		// When an error occurs, notify the handler after MaxAttempts and do
		// exponential backoff until caller explicitly cancels context. This
		// accomplishes a few things:
		// 1) prevent tight-loop if the handler isn't cancelling the context
		// 2) provides back-pressure when consul is down
		// 3) gives the handler visibility to all errors
		if err != nil {
			attempt++
			if attempt <= w.MaxAttempts {
				continue
			}

			// notify the handler after MaxAttempts
			handler(nil, err)

			// exponential backoff to prevent tight-loop when the caller has not
			// cancelled the context
			timer := time.NewTimer(time.Duration(attempt<<1) * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
			}
			timer.Stop()
			continue
		}

		// successful update
		attempt = 0
		handler(resp, err)
	}
}
