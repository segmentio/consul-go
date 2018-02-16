package consul

import (
	"context"
	"net/http"
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
	go Watch(ctx, "test2/key", func(d []KeyData, err error) {
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
	skipFirst := true
	ts := &http.Transport{
		ResponseHeaderTimeout: 100 * time.Microsecond,
	}
	c := &Client{
		Transport: ts,
	}
	w := &Watcher{Client: c, MaxAttempts: 10}
	go w.Watch(ctx, "test3/key", func(d []KeyData, err error) {
		// We should only see this function 2x: first for initialization,
		// then the timeout.  we never trigger the watch and all the errors
		// are temporary
		if skipFirst {
			skipFirst = false
			return
		}
		if ev, ok := err.(interface {
			Temporary() bool
		}); !ok || !ev.Temporary() {
			t.Errorf("Expected Temporary (timeout) error but got: %v", err)
		}
		cancel()
		close(ch)
	})
	<-ch
}

// This tests two things:
// 1. a non-existant key doesn't immediately throw a 404 on the second
//    call (the first will always throw 404 because it doesn't exist)
// 2. the index is being set properly in the request
func TestWatchPrefixNonExistant(t *testing.T) {
	ctx := context.Background()
	ch := make(chan []KeyData, 1)
	skipFirst := true

	go WatchPrefix(ctx, "test4/key", func(d []KeyData, err error) {
		if skipFirst {
			skipFirst = false
			return
		}
		if err != nil {
			t.Error(err)
		}
		ch <- d
	})

	// Give time for the handler to setup, the handler will trigger if there's
	// an error.
	time.Sleep(10 * time.Millisecond)

	// release the test
	err := DefaultClient.Put(ctx, "/v1/kv/test4/key", nil, "narg", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-ch
}
