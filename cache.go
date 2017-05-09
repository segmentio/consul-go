package consul

import (
	"sync"
	"sync/atomic"
	"time"
)

// Implementation of a non-blocking, temporary, cache for values of arbitrary
// types.
//
// Instances of cachedValue are safe to use concurrently from multiple
// goroutines.
type cachedValue struct {
	state atomic.Value
	once  sync.Once
}

// Helper type for the cachedValue implementation.
type cachedValueState struct {
	lock     uint32
	value    interface{}
	error    error
	expireAt time.Time
}

func (cache *cachedValue) lookup(now time.Time, exp time.Time, update func() (interface{}, error)) (interface{}, error) {
	state := cache.load()

	// A nil state indicate that the value has never been set yet, this is the
	// only fully blocking situation since all goroutines must wait on the value
	// to be initialized before they can read it.
	if state == nil {
		cache.once.Do(func() {
			val, err := update()
			cache.store(&cachedValueState{value: val, error: err, expireAt: exp})
		})
		state = cache.load()
	}

	// When the value has expired, only one goroutine will be in charge of
	// fetching and setting an updated value, which is controlled by acquiring
	// the atomic lock and doesn't block the concurrent execution of other
	// goroutines.
	if now.After(state.expireAt) {
		if atomic.CompareAndSwapUint32(&state.lock, 0, 1) {
			val, err := update()

			// If an error occurred while trying to get an updated value we
			// simply keep serving the previous value instead of discarding
			// the cache.
			//
			// TODO: figure out how to report the error?
			if err == nil {
				state = &cachedValueState{value: val, error: err, expireAt: exp}
				cache.store(state)
			}

			atomic.StoreUint32(&state.lock, 0)
		}
	}

	return state.value, state.error
}

func (cache *cachedValue) load() *cachedValueState {
	state, _ := cache.state.Load().(*cachedValueState)
	return state
}

func (cache *cachedValue) store(state *cachedValueState) {
	cache.state.Store(state)
}
