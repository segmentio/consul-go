package consul

import (
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/objconv/json"
)

func testSession(t *testing.T) {
	t.Run("WithSession", testWithSession)
	t.Run("CreateSession", testCreateSession)
	t.Run("DeleteSession", testDeleteSession)
	t.Run("ReadSession", testReadSession)
	t.Run("ListSessionsForNode", testListSessionsForNode)
	t.Run("ListSessions", testListSessions)
	t.Run("RenewSession", testRenewSession)
	t.Run("RenewClientSession", testRenewClientSession)
}

func testWithSession(t *testing.T) {
	config := SessionConfig{
		LockDelay: S(10 * time.Second),
		Name:      "foo",
		Node:      "bar",
		Checks:    []string{"A", "B", "C"},
		Behavior:  Release,
		TTL:       S(1 * time.Minute),
	}
	ref := SessionID("adf4238a-882b-9ddc-4a9d-5b6758e4159e")

	server, client := newServerClient(func(res http.ResponseWriter, req *http.Request) {
		if req.Method != "PUT" {
			t.Error(req.Method)
		}
		if req.URL.Path != "/v1/session/create" {
			t.Error(req.URL.Path)
		}
		json.NewEncoder(res).Encode(struct{ ID SessionID }{ref})
	})
	defer server.Close()

	cli, err := client.WithSession(nil, config)
	if err != nil {
		t.Error(err)
	} else if cli.Session != ref {
		t.Error(cli.Session)
	}
}

func testCreateSession(t *testing.T) {
	config := SessionConfig{
		LockDelay: S(10 * time.Second),
		Name:      "foo",
		Node:      "bar",
		Checks:    []string{"A", "B", "C"},
		Behavior:  Release,
		TTL:       S(1 * time.Minute),
	}
	ref := SessionID("adf4238a-882b-9ddc-4a9d-5b6758e4159e")

	server, client := newServerClient(func(res http.ResponseWriter, req *http.Request) {
		if req.Method != "PUT" {
			t.Error(req.Method)
		}
		if req.URL.Path != "/v1/session/create" {
			t.Error(req.URL.Path)
		}
		json.NewEncoder(res).Encode(struct{ ID SessionID }{ref})
	})
	defer server.Close()

	sid, err := client.CreateSession(nil, config)
	if err != nil {
		t.Error(err)
	} else if sid != ref {
		t.Error(sid)
	}
}

func testDeleteSession(t *testing.T) {
	const ref = SessionID("adf4238a-882b-9ddc-4a9d-5b6758e4159e")

	server, client := newServerClient(func(res http.ResponseWriter, req *http.Request) {
		if req.Method != "PUT" {
			t.Error(req.Method)
		}
		if req.URL.Path != "/v1/session/destroy/"+string(ref) {
			t.Error(req.URL.Path)
		}
	})
	defer server.Close()

	if err := client.DeleteSession(nil, ref); err != nil {
		t.Error(err)
	}
}

func testReadSession(t *testing.T) {
	const ref = SessionID("adf4238a-882b-9ddc-4a9d-5b6758e4159e")
	var info = []Session{{
		LockDelay:   1.5e+10,
		Checks:      []string{"serfHealth"},
		Node:        "foobar",
		ID:          ref,
		CreateIndex: 1086449,
	}}

	server, client := newServerClient(func(res http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			t.Error(req.Method)
		}
		if req.URL.Path != "/v1/session/info/"+string(ref) {
			t.Error(req.URL.Path)
		}
		json.NewEncoder(res).Encode(info)
	})
	defer server.Close()

	s, err := client.ReadSession(nil, ref)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(s, info) {
		t.Errorf("%#v", s)
	}
}

func testListSessionsForNode(t *testing.T) {
	const node = "node1"
	const ref = SessionID("adf4238a-882b-9ddc-4a9d-5b6758e4159e")
	var info = []Session{{
		LockDelay:   1.5e+10,
		Checks:      []string{"serfHealth"},
		Node:        "foobar",
		ID:          ref,
		CreateIndex: 1086449,
	}}

	server, client := newServerClient(func(res http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			t.Error(req.Method)
		}
		if req.URL.Path != "/v1/session/node/"+node {
			t.Error(req.URL.Path)
		}
		json.NewEncoder(res).Encode(info)
	})
	defer server.Close()

	s, err := client.ListSessionsForNode(nil, node)
	if err != nil {
		t.Error(err)
	} else if !reflect.DeepEqual(s, info) {
		t.Errorf("%#v", s)
	}
}

func testListSessions(t *testing.T) {
	const ref = SessionID("adf4238a-882b-9ddc-4a9d-5b6758e4159e")
	var info = []Session{{
		LockDelay:   1.5e+10,
		Checks:      []string{"serfHealth"},
		Node:        "foobar",
		ID:          ref,
		CreateIndex: 1086449,
	}}

	server, client := newServerClient(func(res http.ResponseWriter, req *http.Request) {
		if req.Method != "GET" {
			t.Error(req.Method)
		}
		if req.URL.Path != "/v1/session/list" {
			t.Error(req.URL.Path)
		}
		json.NewEncoder(res).Encode(info)
	})
	defer server.Close()

	s, err := client.ListSessions(nil)
	if err != nil {
		t.Error(err)
	} else if !reflect.DeepEqual(s, info) {
		t.Errorf("%#v", s)
	}
}

func testRenewSession(t *testing.T) {
	const ref = SessionID("adf4238a-882b-9ddc-4a9d-5b6758e4159e")

	server, client := newServerClient(func(res http.ResponseWriter, req *http.Request) {
		if req.Method != "PUT" {
			t.Error(req.Method)
		}
		if req.URL.Path != "/v1/session/renew/"+string(ref) {
			t.Error(req.URL.Path)
		}
	})
	defer server.Close()

	if err := client.RenewSession(nil, ref); err != nil {
		t.Error(err)
	}
}

func testRenewClientSession(t *testing.T) {
	config := SessionConfig{
		LockDelay: S(10 * time.Second),
		Name:      "foo",
		Node:      "bar",
		Checks:    []string{"A", "B", "C"},
		Behavior:  Release,
		TTL:       S(1 * time.Minute),
	}
	ref := SessionID("adf4238a-882b-9ddc-4a9d-5b6758e4159e")
	cnt := 0

	server, client := newServerClient(func(res http.ResponseWriter, req *http.Request) {
		if req.Method != "PUT" {
			t.Error(req.Method)
		}
		if cnt == 0 {
			if req.URL.Path != "/v1/session/create" {
				t.Error(req.URL.Path)
			}
			json.NewEncoder(res).Encode(struct{ ID SessionID }{ref})
		} else {
			if req.URL.Path != "/v1/session/renew/"+string(ref) {
				t.Error(req.URL.Path)
			}
		}
		cnt++
	})
	defer server.Close()

	other, err := client.WithSession(nil, config)
	if err != nil {
		t.Error(err)
	} else if err := other.RenewClientSession(nil); err != nil {
		t.Error(err)
	}
}
