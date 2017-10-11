package consul

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/segmentio/objconv"
	"github.com/segmentio/objconv/json"
)

// A Store exposes an API to interract with the consul key/value store.
type Store struct {
	// The client used to send requests to the consul agent.
	Client *Client

	// A key prefix to apply to all operations made on this store.
	Keyspace string
}

// Tree recursively scans the given key prefix in the consul key/value store,
// returning the list of keys as a slice of strings.
//
// If the consul key/value store contains a large number of keys under the given
// prefix, one may prefer to use the Walk method to iterate through the keys
// without loading them all in memory.
func (store *Store) Tree(ctx context.Context, prefix string) (keys []string, err error) {
	err = store.client().Get(ctx, store.path(prefix), Query{{Name: "keys"}}, &keys)
	for i := range keys {
		keys[i] = store.clean(keys[i])
	}
	return
}

// Walk traverses the keyspace under the given prefix, calling the walk function
// and passing each key.
//
// If the walk function returns an error the iteration is stopped and the error
// is returned by Walk.
func (store *Store) Walk(ctx context.Context, prefix string, walk func(key string) error) (err error) {
	var result io.ReadCloser
	var query = Query{
		{Name: "keys"},
		{Name: "recurse", Value: "true"},
	}

	if _, result, err = store.client().do(ctx, "GET", store.path(prefix), query, nil); err != nil {
		return
	}
	defer result.Close()

	var stream = json.NewStreamDecoder(result)
	var key string

	for stream.Decode(&key) == nil {
		if err = walk(store.clean(key)); err != nil {
			return
		}
	}

	err = stream.Err()
	return
}

// KeyData is a representation of a key in Consul, which follows the structure
// documented at https://www.consul.io/api/kv.html#read-key
type KeyData struct {
	CreateIndex int64
	ModifyIndex int64
	Key         string
	Flags       int64
	Value       []byte
	Session     SessionID
}

// WalkData traverses the keyspace under the given prefix, calling the walk
// function and passing each key's data
//
// If the walk function returns an error the iteration is stopped and the error
// is returned by WalkData
func (store *Store) WalkData(ctx context.Context, prefix string, walk func(data KeyData) error) (err error) {
	var result io.ReadCloser
	var query = Query{
		{Name: "recurse", Value: "true"},
	}

	if _, result, err = store.client().do(ctx, "GET", store.path(prefix), query, nil); err != nil {
		return
	}
	defer result.Close()

	var stream = json.NewStreamDecoder(result)
	var data KeyData
	for stream.Decode(&data) == nil {
		data.Key = store.clean(data.Key)
		if err = walk(data); err != nil {
			return
		}
	}

	err = stream.Err()
	return
}

// Read reads the value of the given key, returning it as a pair of an
// io.ReadCloser and value of the last index that modified the key.
//
// The program must close the value when it's done reading from it to prevent
// any leak of internal resources.
func (store *Store) Read(ctx context.Context, key string) (value io.ReadCloser, index int64, err error) {
	var header http.Header
	var sindex string

	if header, value, err = store.client().do(ctx, "GET", store.path(key), Query{{Name: "raw"}}, nil); err != nil {
		return
	}

	if sindex = header.Get("X-Consul-Index"); len(sindex) == 0 {
		value.Close()
		err = errMissingXConsulIndex
		return
	}

	if index, err = strconv.ParseInt(sindex, 10, 64); err != nil {
		value.Close()
		err = fmt.Errorf("bad X-Consul-Index in HTTP response: %s", sindex)
		return
	}

	return
}

// ReadValue reads the JSON-encoded value at the given key into ptr. The usual
// unmarshaling rules apply.
//
// See Read for more details on the method.
func (store *Store) ReadValue(ctx context.Context, key string, ptr interface{}) (index int64, err error) {
	var result io.ReadCloser

	if result, index, err = store.Read(ctx, key); err != nil {
		return
	}
	defer result.Close()

	err = json.NewDecoder(result).Decode(ptr)
	return
}

// Write writes bytes from value at the given key in the consul key/value store.
//
// If index is set to a positive value, it is used to turn the write call into a
// compare-and-swap operation. The value will only be updated if the last index
// that modified the key matches the given index.
//
// If the given context inherits from a lock, the associated session is used to
// allow the write operation to succeed.
//
// The method returns a boolean that indicates whether the write operation
// succeeded, it would return false if the key was locked by another session
// or if index was specified but was different in consul.
//
// The value is always closed by a call to Write, even if the method returns an
// error.
func (store *Store) Write(ctx context.Context, key string, value io.ReadCloser, index int64) (ok bool, err error) {
	var result io.ReadCloser
	var query Query

	locks, _ := ctx.Value(LocksKey).([]string)
	for _, lock := range locks {
		if lock = store.clean(lock); lock == key {
			query = append(query, Param{
				Name:  "acquire",
				Value: string(contextSession(ctx).ID),
			})
		}
	}

	if index > 0 {
		query = append(query, Param{
			Name:  "cas",
			Value: strconv.FormatInt(index, 10),
		})
	}

	if _, result, err = store.client().do(ctx, "PUT", store.path(key), query, value); err != nil {
		return
	}
	defer result.Close()

	err = json.NewDecoder(result).Decode(&ok)
	return
}

// WriteValue writes the JSON representation of value at the given key.
// The usual marshaling rules apply.
//
// See Write for more details on the method.
func (store *Store) WriteValue(ctx context.Context, key string, value interface{}, index int64) (ok bool, err error) {
	// Use a pretty-JSON encoder to make it easier to read values in the consul
	// web UI.
	b := &bytes.Buffer{}
	e := objconv.Encoder{Emitter: json.NewPrettyEmitter(b)}

	if err = e.Encode(value); err != nil {
		return
	}

	ok, err = store.Write(ctx, key, ioutil.NopCloser(b), index)
	return
}

// Delete deletes the keys stored under the given prefix. If index is set to a
// positive value, it is used to turn the delete call into a compare-and-swap
// operation where the keys are only deleted if the last index that modified
// them matches the given index.
//
// The method returns a boolean to indicate whether the keys could be deleted.
func (store *Store) Delete(ctx context.Context, prefix string, index int64) (ok bool, err error) {
	query := Query{{"recurse", "true"}}

	if index > 0 {
		query = append(query, Param{
			Name:  "cas",
			Value: strconv.FormatInt(index, 10),
		})
	}

	err = store.client().Delete(ctx, store.path(prefix), query, &ok)
	return
}

// Session return the session used to lock the given key. It returns an error
// if the key is not locked or does not exist.
func (store *Store) Session(ctx context.Context, key string) (session Session, err error) {
	var keyData KeyData

	if keyData, err = store.readKeyData(ctx, key); err != nil {
		return
	}

	if len(keyData.Session) == 0 {
		err = fmt.Errorf("key %s is not locked", key)
		return
	}

	var client = store.client()
	var configs []sessionConfig
	var path = "/v1/session/info/" + string(keyData.Session)

	if err = client.Get(ctx, path, nil, &configs); err != nil {
		return
	}

	if len(configs) == 0 {
		err = fmt.Errorf("key %s is not locked", key)
		return
	}

	config := configs[0]
	session = Session{
		Client:    client,
		ID:        keyData.Session,
		Name:      config.Name,
		Behavior:  SessionBehavior(config.Behavior),
		LockDelay: fromSeconds(config.LockDelay),
		TTL:       fromSeconds(config.TTL),
	}
	return
}

func (store *Store) readKeyData(ctx context.Context, key string) (keyData KeyData, err error) {
	var meta []KeyData

	if err = store.client().Get(ctx, store.path(key), nil, &meta); err != nil {
		return
	}

	if len(meta) == 0 {
		err = fmt.Errorf("key %s does not exist", key)
		return
	}

	keyData = meta[0]
	return
}

func (store *Store) client() *Client {
	if client := store.Client; client != nil {
		return client
	}
	return DefaultClient
}

func (store *Store) path(key string) string {
	return formatKVPath(store.Keyspace, key)
}

func (store *Store) clean(key string) string {
	key = strings.TrimPrefix(key, store.Keyspace)
	key = strings.TrimPrefix(key, "/")
	return key
}

// Our own snowflake cleanPath, derived from net/http/server.go
func cleanPath(p string) string {
	if p == "" {
		return ""
	}

	// Convert trailing /. to /, so path.Clean doesn't get to it
	if strings.HasSuffix(p, "/.") {
		p = p[:len(p)-1]
	}

	if p[0] != '/' {
		p = "/" + p
	}
	np := path.Clean(p)
	// path.Clean removes trailing slash except for root;
	// put the trailing slash back if necessary.
	if p[len(p)-1] == '/' && np != "/" {
		np += "/"
	}
	return np
}

func formatKVPath(prefix, key string) string {
	joined := cleanPath("/v1/kv") + "/" + cleanPath(prefix) + "/" + cleanPath(key)

	// collapse slashes
	out := []rune{}
	var last rune
	for _, runeValue := range joined {
		if runeValue == '/' && last == '/' {
			continue
		}
		out = append(out, runeValue)
		last = runeValue
	}

	return string(out)
}

var (
	errMissingXConsulIndex = errors.New("missing X-Consul-Index in HTTP response")
)
