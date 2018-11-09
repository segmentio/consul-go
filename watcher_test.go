package consul

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestWatchPrefix(t *testing.T) {
	ctx := context.Background()
	err := DefaultClient.Put(ctx, "/v1/kv/test1/key", nil, "blah", nil)
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan struct{})
	res := []KeyData{}
	skipFirst := true
	go WatchPrefix(ctx, "test1/key", func(d []KeyData, err error) {
		if skipFirst {
			skipFirst = false
			return
		}
		res = d
		close(ch)
	})
	// Give time for the handler to setup
	time.Sleep(10 * time.Millisecond)
	err = DefaultClient.Put(ctx, "/v1/kv/test1/key", nil, "narg", nil)
	if err != nil {
		t.Fatal(err)
	}

	<-ch
	if string(res[0].Value) != "\"narg\"" {
		t.Errorf("watch should return updated value. exp: \"narg\", act: %v", string(res[0].Value))
	}
}

func TestWatch(t *testing.T) {
	ctx := context.Background()
	err := DefaultClient.Put(ctx, "/v1/kv/test2/key", nil, "blah", nil)
	if err != nil {
		t.Fatal(err)
	}
	res := KeyData{}
	ch := make(chan struct{})
	skipFirst := true
	w := &Watcher{MaxAttempts: 2, MaxBackoff: 10 * time.Millisecond}
	go w.Watch(ctx, "test2/key", func(d []KeyData, err error) {
		if skipFirst {
			skipFirst = false
			return
		}
		res = d[0]
		close(ch)
	})
	// Give time for the handler to setup
	time.Sleep(10 * time.Millisecond)
	err = DefaultClient.Put(ctx, "/v1/kv/test2/key", nil, "narg", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-ch
	if string(res.Value) != "\"narg\"" {
		t.Log(len(res.Value))
		t.Log(len(`"narg"`))
		for i := range res.Value {
			t.Logf("%c (%#x)", res.Value[i], res.Value[i])
		}
		t.Errorf("watch should return updated value. exp: \"narg\", act: %v", string(res.Value))
	}
}

func TestWatchTimeoutMaxAttempts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	err := DefaultClient.Put(ctx, "/v1/kv/test3/key", nil, "blah", nil)
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan struct{})
	ts := &http.Transport{ResponseHeaderTimeout: 100 * time.Microsecond}
	c := &Client{Transport: ts}
	w := &Watcher{Client: c, MaxAttempts: 2, MaxBackoff: 10 * time.Millisecond}
	go w.Watch(ctx, "test3/key", func(d []KeyData, err error) {
		if ev, ok := err.(interface {
			Timeout() bool
		}); !ok || !ev.Timeout() {
			t.Errorf("Expected timeout error but got: %v", err)
		}
		// this might fire more than once and try to close the channel twice
		// unless we cancel the context
		cancel()
		close(ch)
	})
	<-ch
}

// This tests two things:
// 1. a non-existant key doesn't receive an error
// 2. the index is being set properly in the request
func TestWatchPrefixNonExistant(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// ensure that the key does not exist
	if err := DefaultClient.Delete(ctx, "/v1/kv/test4/key", nil, nil); err != nil {
		t.Fatal(err)
	}
	ch := make(chan struct{})
	go WatchPrefix(ctx, "test4/key", func(d []KeyData, err error) {
		if err != nil {
			t.Fatal(err)
		}
		if len(d) != 0 {
			t.Fatal("data should be empty")
		}
		cancel()
		close(ch)
	})
	<-ch
}

func TestWatchMaxBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan struct{})
	c := &Client{Transport: &mockTransport{500, nil, nil}}
	maxAttempts := 1
	initialBackoff := 1 * time.Hour
	maxBackoff := 10 * time.Millisecond
	w := &Watcher{
		Client:         c,
		MaxAttempts:    maxAttempts,
		InitialBackoff: initialBackoff,
		MaxBackoff:     maxBackoff,
	}
	start := time.Now()
	go w.Watch(ctx, "test5/key", func(d []KeyData, err error) {
		// this might fire more than once and try to close the channel twice
		// unless we cancel the context
		cancel()
		close(ch)
	})
	<-ch

	elapsed := time.Now().Sub(start)

	// execution time should never be more than twice the backoff
	if elapsed > maxBackoff*2 {
		t.Errorf("watcher backoff was longer than expected, test took %v", elapsed)
	}
}

func TestWatchErrorMaxAttempts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	if err := DefaultClient.Put(ctx, "/v1/kv/test6/key", nil, "blah", nil); err != nil {
		t.Fatal(err)
	}
	ch := make(chan struct{})
	c := &Client{Transport: &mockTransport{500, nil, nil}}
	w := &Watcher{Client: c, MaxAttempts: 5, MaxBackoff: 10 * time.Millisecond}
	// this will continue to fire after max attempts is reached so need to
	// cancel to prevent calling close a second time.
	go w.Watch(ctx, "test6/key", func(d []KeyData, err error) {
		if err == nil {
			t.Fatal("Expected error but got nil")
		}
		cancel()
		close(ch)
	})
	<-ch
}

type mockTransport struct {
	StatusCode int
	Headers    map[string]string
	Body       []byte
}

func (t *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	status := strconv.Itoa(t.StatusCode) + " " + http.StatusText(t.StatusCode)
	header := http.Header{}
	for name, value := range t.Headers {
		header.Set(name, value)
	}

	contentLength := len(t.Body)
	header.Set("Content-Length", strconv.Itoa(contentLength))

	res := &http.Response{
		Status:           status,
		StatusCode:       t.StatusCode,
		Proto:            "HTTP/1.0",
		ProtoMajor:       1,
		ProtoMinor:       0,
		Header:           header,
		Body:             ioutil.NopCloser(bytes.NewReader(t.Body)),
		ContentLength:    int64(contentLength),
		TransferEncoding: []string{},
		Close:            false,
		Uncompressed:     false,
		Trailer:          nil,
		Request:          req,
		TLS:              nil,
	}

	// no Content-Length when 204 or 304
	if t.StatusCode == http.StatusNoContent || t.StatusCode == http.StatusNotModified {
		if res.ContentLength != 0 {
			res.Body = ioutil.NopCloser(bytes.NewReader([]byte{}))
			res.ContentLength = 0
		}
		header.Del("Content-Length")
	}
	return res, nil
}
