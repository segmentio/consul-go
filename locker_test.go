package consul

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTryLock(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	keys := []string{
		"test-trylock-1",
		"test-trylock-2",
		"test-trylock-3",
	}

	for i := 0; i != len(keys); i++ {
		lock, unlock := TryLock(ctx, keys...)
		defer unlock()

		lockKeys := lock.Value(LocksKey)
		ok := false

		for _, k1 := range keys {
			for _, k2 := range lockKeys.([]string) {
				if k1 == k2 {
					ok = true
					break
				}
			}
		}

		if !ok {
			t.Error("bad lock keys:", lockKeys)
		}
	}

	lock, unlock := TryLock(ctx, keys...)
	defer unlock()

	select {
	case <-lock.Done():
	default:
		t.Error("too many locks got successfully acquired")
	}
}

func TestLock(t *testing.T) {
	t.Parallel()
	concurrency := int32(0)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	wg := sync.WaitGroup{}

	for i := 0; i != 3; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			t.Logf("[%02d] lock", i)
			lock, unlock := Lock(ctx, "test-lock-1", "test-lock-2", "test-lock-3")
			defer unlock()

			if n := int(atomic.AddInt32(&concurrency, +1)); n > 1 {
				t.Errorf("concurrency is too high at %d instead of 1", n)
			}

			select {
			case <-lock.Done():
			case <-time.After(50 * time.Millisecond):
			}

			atomic.AddInt32(&concurrency, -1)
			t.Logf("[%02d] unlock\n", i)
		}(i)
	}

	wg.Wait()
}
