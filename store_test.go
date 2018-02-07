package consul

import (
	"context"
	"errors"
	"fmt"
	"path"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestStore(t *testing.T) {
	tests := []struct {
		scenario string
		test     func(*testing.T, context.Context, *Store)
	}{
		{
			scenario: "write a couple of keys and ensure they show up when listing the key/value store",
			test:     testWriteAndTree,
		},

		{
			scenario: "write a couple of keys and ensure they show up when walking the key/value store",
			test:     testWriteAndWalk,
		},

		{
			scenario: "write a couple of keys and ensure they show up when walking the key/value store and returning data",
			test:     testWriteAndWalkData,
		},

		{
			scenario: "write a key and verify that reading the value returns the right content",
			test:     testWriteAndRead,
		},

		{
			scenario: "compare-and-swap a value must fail if the last modification index differs",
			test:     testCompareAndSwapFailure,
		},

		{
			scenario: "compare-and-swap a value must succeed if the last modification index matches",
			test:     testCompareAndSwapSuccess,
		},

		{
			scenario: "compare-and-swap a nonexistent value must fail if the value exists",
			test:     testCompareAndSwapNonexistentFailure,
		},

		{
			scenario: "compare-and-swap a nonexistent value must succeed if value does not exist",
			test:     testCompareAndSwapNonexistentSuccess,
		},

		{
			scenario: "writing a locked key without passing the lock context should fail",
			test:     testWriteLockedKeyFailure,
		},

		{
			scenario: "writing a locked key while passing the lock context should succeed",
			test:     testWriteLockedKeySuccess,
		},
		{
			scenario: "should read a session from a locked key",
			test:     testSessionSuccess,
		},
		{
			scenario: "read a session from a not locked key should fail",
			test:     testSessionFailure,
		},
		{
			scenario: "walk from a not set key should return a not found error",
			test:     testWalkFromUnsetKey,
		},
	}

	counter := int32(0)

	for _, test := range tests {
		testFunc := test.test
		t.Run(test.scenario, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			store := &Store{
				Keyspace: fmt.Sprintf("test-store-%d", atomic.AddInt32(&counter, 1)),
			}

			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()

				if ok, err := store.Delete(ctx, "", -1); err != nil {
					t.Error(err)
				} else if !ok {
					t.Error("failed to delete test keyspace", store.Keyspace)
				}
			}()
			testFunc(t, ctx, store)
		})
	}
}

func testWriteAndTree(t *testing.T, ctx context.Context, store *Store) {
	keys := []string{"A/1", "A/2", "B/1", "C/1", "A/3"}

	for i, key := range keys {
		write(t, ctx, store, key, i, -1)
	}

	tree, err := store.Tree(ctx, "A")
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(tree, []string{"A/1", "A/2", "A/3"}) {
		t.Error("bad keys:", tree)
	}
}

func testWriteAndWalk(t *testing.T, ctx context.Context, store *Store) {
	keys := []string{"A/1", "A/2", "B/1", "C/1", "A/3"}

	for i, key := range keys {
		write(t, ctx, store, key, i, -1)
	}

	i := 0
	ref := []string{"A/1", "A/2", "A/3"}
	err := store.Walk(ctx, "A", func(key string) error {
		if key != ref[i] {
			return fmt.Errorf("bad key at index %d: %q != %q", i, key, ref[i])
		}
		i++
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func testWriteAndWalkData(t *testing.T, ctx context.Context, store *Store) {
	keys := []string{"A/1", "A/2", "B/1", "C/1", "A/3"}

	for i, key := range keys {
		write(t, ctx, store, key, i, -1)
	}

	i := 0
	ref := []string{"A/1", "A/2", "A/3"}
	err := store.WalkData(ctx, "A", func(data KeyData) error {
		if data.Key != ref[i] {
			return fmt.Errorf("bad key at index %d: %q != %q", i, data.Key, ref[i])
		}
		if data.Flags != 0 {
			return fmt.Errorf("unexpected Flags value of %q", data.Flags)
		}
		if data.CreateIndex <= 0 {
			return fmt.Errorf("CreateIndex should be > 0")
		}
		if data.ModifyIndex != data.CreateIndex {
			return fmt.Errorf("ModifyIndex should == CreateIndex")
		}

		// I blame Brad for the next 10 lines of code
		kidx := -1
		for keysi, v := range keys {
			if v == data.Key {
				kidx = keysi
				break
			}
		}
		if kidx == -1 {
			return fmt.Errorf("This shouldn't happen.")
		}

		dvi, _ := strconv.Atoi(string(data.Value))
		if dvi != kidx {
			return fmt.Errorf("Value should be the index %q, but it is %q", kidx, dvi)
		}

		i++
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func testWriteAndRead(t *testing.T, ctx context.Context, store *Store) {
	type T struct {
		Hello  string
		Answer int
	}

	v1 := T{"World", 42}
	v2 := T{}
	write(t, ctx, store, "my-key", v1, -1)

	if _, err := store.ReadValue(ctx, "my-key", &v2); err != nil {
		t.Error("reading my-key failed:", err)
	}

	if !reflect.DeepEqual(v1, v2) {
		t.Error("readin my-key returned an bad value:")
		t.Logf("<<< %#v", v1)
		t.Logf(">>> %#v", v2)
	}
}

func testCompareAndSwapFailure(t *testing.T, ctx context.Context, store *Store) {
	write(t, ctx, store, "A", 42, -1)

	value := int(0)
	index, err := store.ReadValue(ctx, "A", &value)
	if err != nil {
		t.Error(err)
		return
	}

	// Updating the key will change the last modification index.
	write(t, ctx, store, "A", 1, -1)

	// Attempting a compare-and-swap should fail.
	if ok, err := store.WriteValue(ctx, "A", 2, index); err != nil {
		t.Error(err)
	} else if ok {
		t.Error("the CAS operation should have failed because the last modification index has been changed")
	}
}

func testCompareAndSwapSuccess(t *testing.T, ctx context.Context, store *Store) {
	write(t, ctx, store, "A", 42, -1)

	value := int(0)
	index, err := store.ReadValue(ctx, "A", &value)
	if err != nil {
		t.Error(err)
		return
	}

	// Attempting a compare-and-swap should succeed because the key hasn't been
	// modified.
	write(t, ctx, store, "A", 1, index)

	if _, err := store.ReadValue(ctx, "A", &value); err != nil {
		t.Error(err)
	} else if value != 1 {
		t.Error("bad value after CAS operation:", value)
	}
}

func testCompareAndSwapNonexistentFailure(t *testing.T, ctx context.Context, store *Store) {
	write(t, ctx, store, "A", 42, -1)

	// Attempting a compare-and-swap should fail if the value exists.
	if ok, err := store.WriteValue(ctx, "A", 2, 0); err != nil {
		t.Error(err)
	} else if ok {
		t.Error("the CAS operation should have failed because the value existed")
	}
}

func testCompareAndSwapNonexistentSuccess(t *testing.T, ctx context.Context, store *Store) {
	value := 1

	// Attempting a compare-and-swap should succeed because the key doesn't exist
	write(t, ctx, store, "A", value, 0)

	if _, err := store.ReadValue(ctx, "A", &value); err != nil {
		t.Error(err)
	} else if value != 1 {
		t.Error("bad value after CAS operation:", value)
	}
}

func testWriteLockedKeyFailure(t *testing.T, ctx context.Context, store *Store) {
	_, unlock := (&Locker{Client: store.Client, Keyspace: store.Keyspace}).Lock(ctx, "A")
	defer unlock()

	session, expire := WithSession(ctx, Session{})
	defer expire()
	// Build a context that pretends to be a lock on A from a different session.
	ctx = context.WithValue(session, LocksKey, []string{store.Keyspace + "/A"})

	// Attempting to write the keys without passing the session holding the lock
	// should fail.
	if ok, err := store.WriteValue(ctx, "A", 2, -1); err != nil {
		t.Error(err)
	} else if ok {
		t.Error("the write operation passed the wrong lock as context therefore it should have failed")
	}
}

func testWriteLockedKeySuccess(t *testing.T, ctx context.Context, store *Store) {
	lock, unlock := (&Locker{Client: store.Client, Keyspace: store.Keyspace}).Lock(ctx, "A")
	defer unlock()

	// Passing the lock should allow the key to be written.
	if ok, err := store.WriteValue(lock, "A", 2, -1); err != nil {
		t.Error(err)
	} else if !ok {
		t.Error("the write operation passed the lock as context therefore it should have succeeded")
	}
}

func write(t *testing.T, ctx context.Context, store *Store, key string, value interface{}, index int64) {
	if ok, err := store.WriteValue(ctx, key, value, index); err != nil {
		t.Fatalf("writing %s=%v failed (%s)", key, value, err)
	} else if !ok {
		t.Fatalf("writing %s=%v failed (WriteValue returned false)", key, value)
	}
}

func testSessionSuccess(t *testing.T, ctx context.Context, store *Store) {
	key := "foo/lock"
	lock, unlock := TryLockOne(ctx, path.Join(store.Keyspace, key))
	defer unlock()

	if err := lock.Err(); err != nil {
		t.Error(err)
	}

	_, err := store.Session(ctx, key)
	if err != nil {
		t.Error(err)
	}
}

func testSessionFailure(t *testing.T, ctx context.Context, store *Store) {
	write(t, ctx, store, "bar/lock", "bar", -1)

	_, err := store.Session(ctx, "bar/lock")
	if err == nil {
		t.Fatal("err should not be nil")
	}
	t.Log(err)
}

func testWalkFromUnsetKey(t *testing.T, ctx context.Context, store *Store) {
	err := store.Walk(ctx, "foo/bar/kada/bra", func(key string) error {
		return errors.New("the Walk callback was called for a key that did not exist")
	})

	notFound, ok := err.(errNotFound)
	if !ok {
		t.Error("the error returned doesn't satisfies the errNotFound interface")
	}
	if !notFound.NotFound() {
		t.Error("NotFound() does not return true")
	}
}

type errNotFound interface {
	NotFound() bool
}

func TestFormatKVPath(t *testing.T) {
	assertKVP := func(expected, prefix, key string) {
		got := formatKVPath(prefix, key)
		if got != expected {
			t.Errorf("For [prefix:%v, key:%v], expected:%v, got:%v", prefix, key, expected, got)
		}
	}

	// collapse slashes
	assertKVP("/v1/kv/prefix/key/", "//prefix///", "//key/////")
	assertKVP("/v1/kv/prefix/key", "//prefix///", "//key")

	// handle trailing slashes properly
	assertKVP("/v1/kv/prefix/key", "prefix", "key")
	assertKVP("/v1/kv/prefix/key", "/prefix", "/key")
	assertKVP("/v1/kv/prefix/key/", "/prefix/", "/key/")
	assertKVP("/v1/kv/prefix/key", "prefix/", "key")
	assertKVP("/v1/kv/prefix/key/", "prefix/", "key/")

	// empty or slash prefix should always result in trailing slash
	assertKVP("/v1/kv/", "", "")
	assertKVP("/v1/kv/", "", "/")
	assertKVP("/v1/kv/", "/", "")
	assertKVP("/v1/kv/", "/", "/")

	// prefixes should always result in a trailing slash
	assertKVP("/v1/kv/prefix/", "/prefix", "")
	assertKVP("/v1/kv/prefix/", "prefix", "")
	assertKVP("/v1/kv/prefix/", "prefix/", "")
	assertKVP("/v1/kv/prefix/", "/prefix/", "")
	assertKVP("/v1/kv/prefix/", "/prefix", "/")
	assertKVP("/v1/kv/prefix/", "prefix", "/")
	assertKVP("/v1/kv/prefix/", "prefix/", "/")
	assertKVP("/v1/kv/prefix/", "/prefix/", "/")

	// collapse but don't allow going below the prefix
	assertKVP("/v1/kv/prefix/whatever", "prefix", "../whatever")
	assertKVP("/v1/kv/prefix/whatever/", "prefix", "../whatever/")
	assertKVP("/v1/kv/prefix/whatever/", "prefix", "../whatever/.")
	assertKVP("/v1/kv/prefix/", "prefix", "../whatever/..")

	// or the prefix dropping below the /v1/kv/ root
	assertKVP("/v1/kv/prefix/", "../prefix", "")
}
