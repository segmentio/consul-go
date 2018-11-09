package consul

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"time"
)

const defMaxAttempts = 10

var (
	defInitialBackoff = 1 * time.Second
	defMaxBackoff     = 30 * time.Second
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
	// bailing out of the watch. Defaults to 10.
	MaxAttempts int

	// InitialBackoff is the amount of time to wait on the initial backoff when
	// an error occurs. Backoff durations are subsequently increased exponentially
	// while less than MaxBackoff. Defaults to 1s.
	InitialBackoff time.Duration

	// MaxBackoff limits the maximum time to wait when doing exponential backoff
	// after encountering errors. Defaults to 30s.
	MaxBackoff time.Duration
}

var (
	DefaultWatcher = &Watcher{
		MaxAttempts:    defMaxAttempts,
		InitialBackoff: defInitialBackoff,
		MaxBackoff:     defMaxBackoff,
	}

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
		w.MaxAttempts = defMaxAttempts
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
			// Not found errors are fine in this context, we have to treat
			// them as non-errors in order to communicate the absence of data
			// to the handler.
			if nfe, ok := err.(*httpError); ok && nfe.NotFound() {
				attempt = 0
				handler(resp, nil)
				continue
			}

			attempt++
			if attempt <= w.MaxAttempts {
				continue
			}

			// notify the handler after MaxAttempts
			handler(nil, err)

			// exponential backoff prevents tight-loop when the caller has not
			// cancelled the context
			w.backoff(ctx, attempt)
			continue
		}

		// successful update
		attempt = 0
		handler(resp, err)
	}
}

func (w *Watcher) backoff(ctx context.Context, n int) {
	if w.InitialBackoff <= 0 {
		w.InitialBackoff = defInitialBackoff
	}
	if w.MaxBackoff <= 0 {
		w.MaxBackoff = defMaxBackoff
	}

	backoff := time.Duration(n<<1) * w.InitialBackoff
	if backoff > w.MaxBackoff {
		backoff = w.MaxBackoff
	}

	timer := time.NewTimer(backoff)

	// wait for either the context to cancel or timer to expire
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
	timer.Stop()
}
