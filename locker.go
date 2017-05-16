package consul

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// A Locker exposes methods for acquiring locks on keys of the consul key/value
// store.
type Locker struct {
	// The client used to send requests to the consul agent. If nil, the default
	// client is used instead.
	Client *Client

	// LockDelay is the amount of time that a lock will stay held if it hasn't
	// been released and the session that was attached to it expired.
	LockDelay time.Duration
}

// Lock acquires locks on the given keys. The method blocks until the locks were
// acquired, or the context was camceled. The returned context will be canceled
// when the locks are released (by calling the cancellation function), or if the
// ownership was lost, in this case the contexts' Err method returns Unlocked.
func (l *Locker) Lock(ctx context.Context, keys ...string) (context.Context, context.CancelFunc) {
	retryInterval := 1 * time.Second
	sessionCtx, sessionCancel := l.withSession(ctx, "lock: %v", keys)

	deadline, ok := ctx.Deadline()
	if ok {
		retryInterval = deadline.Sub(time.Now()) / 10
	}

	locks := make([]ctxCancel, 0, len(keys))
	sortedKeys := sortedKeys(keys)

	for {
		for _, key := range sortedKeys {
			lockCtx, lockCancel, _ := l.tryLock(sessionCtx, key)
			if lockCtx == nil {
				break
			}
			locks = append(locks, ctxCancel{lockCtx, lockCancel})
		}

		if len(locks) == len(keys) {
			switch len(locks) {
			case 0:
				return errorContext(ctx, Unlocked)
			case 1:
				lock := locks[0]
				return lock.ctx, func() { lock.cancel(); sessionCancel() }
			default:
				multi := newMultiCtx(sortedKeys, locks)
				return multi, func() { multi.cancel(); sessionCancel() }
			}
		}

		for _, lock := range locks {
			lock.cancel()
		}
		locks = locks[:0]

		timer := time.NewTicker(retryInterval)
		select {
		case <-timer.C:
			timer.Stop()
		case <-ctx.Done():
			timer.Stop()
			sessionCancel()
			return errorContext(ctx, ctx.Err())
		}
	}
}

// TryLockOne attempts to acquire a lock on one of the given keys. The returned
// context will be canceled when the lock is released (by calling the
// cancellation function), or if the ownership was lost, in this case the
// context's Err method returns Unlocked.
// The method never blocks, if it fails to acquire any of the locks it returns
// a canceled context.
func (l *Locker) TryLockOne(ctx context.Context, keys ...string) (context.Context, context.CancelFunc) {
	var err error
	sessionCtx, sessionCancel := l.withSession(ctx, "try-lock: %v", keys)

	for _, key := range keys {
		var lockCtx context.Context
		var lockCancel context.CancelFunc

		if lockCtx, lockCancel, err = l.tryLock(sessionCtx, key); lockCtx != nil {
			return lockCtx, func() { lockCancel(); sessionCancel() }
		}
	}

	sessionCancel()
	return errorContext(ctx, coalesceError(err, Unlocked))
}

func (l *Locker) tryLock(ctx context.Context, key string) (context.Context, context.CancelFunc, error) {
	client := l.client()
	session := contextSession(ctx)

	tryLockCtx, tryLockCancel := context.WithTimeout(ctx, l.lockDelay())
	defer tryLockCancel()

	locked, err := client.acquireLock(tryLockCtx, key, string(session.ID))
	if !locked || err != nil {
		return nil, nil, err
	}

	lock := newLockCtx(ctx, key, client)
	return lock, lock.cancel, nil
}

func (l *Locker) withSession(ctx context.Context, name string, args ...interface{}) (context.Context, context.CancelFunc) {
	if ctx.Value(SessionKey) != nil {
		// The context that was used to create the lock is already a session, we
		// can use it directly instead of recreating one.
		return ctx, func() {}
	}
	lockDelay := l.lockDelay()
	return WithSession(ctx, Session{
		Client:    l.client(),
		Name:      fmt.Sprintf(name, args...),
		Behavior:  Release,
		LockDelay: lockDelay,
		TTL:       lockDelay * 2,
	})
}

func (l *Locker) client() *Client {
	if client := l.Client; client != nil {
		return client
	}
	return DefaultClient
}

func (l *Locker) lockDelay() time.Duration {
	if delay := l.LockDelay; delay != 0 {
		return delay
	}
	return 15 * time.Second
}

// Lock calls DefaultLocker.Lock.
func Lock(ctx context.Context, keys ...string) (context.Context, context.CancelFunc) {
	return DefaultLocker.Lock(ctx, keys...)
}

// TryLockOne calls DefaultLocker.TryLockOne.
func TryLockOne(ctx context.Context, keys ...string) (context.Context, context.CancelFunc) {
	return DefaultLocker.TryLockOne(ctx, keys...)
}

var (
	// DefaultLocker is the default Locker used by the package-level lock
	// management functions.
	DefaultLocker = &Locker{}

	// LocksKey is used to lookup the keys held by a lock from its associated
	// context.
	LocksKey = &contextKey{"consul-locks-key"}

	// Unlocked is the error returned by contexts when the lock they were
	// associated with has been lost.
	Unlocked = errors.New("unlocked")
)

func newLockCtx(ctx context.Context, key string, client *Client) *lockCtx {
	l := &lockCtx{
		client: client,
		ctx:    ctx,
		key:    key,
		done:   make(chan struct{}),
	}
	go l.run()
	return l
}

type lockCtx struct {
	client *Client
	ctx    context.Context
	key    string
	err    atomic.Value
	once   sync.Once
	done   chan struct{}
}

func (l *lockCtx) Deadline() (time.Time, bool) {
	return l.ctx.Deadline()
}

func (l *lockCtx) Done() <-chan struct{} {
	return l.done
}

func (l *lockCtx) Err() error {
	err, _ := l.err.Load().(error)
	return err
}

func (l *lockCtx) Value(key interface{}) interface{} {
	if key == LocksKey {
		return []string{l.key}
	}
	return l.ctx.Value(key)
}

func (l *lockCtx) cancel() {
	l.cancelWithError(context.Canceled)
}

func (l *lockCtx) cancelWithError(err error) {
	l.once.Do(func() {
		l.err.Store(err)
		close(l.done)

		session := contextSession(l.ctx)
		ctx, cancel := context.WithTimeout(context.Background(), session.LockDelay)
		l.client.releaseLock(ctx, l.key, string(session.ID))
		cancel()
	})
}

func (l *lockCtx) run() {
	select {
	case <-l.done:
	case <-l.ctx.Done():
		l.cancelWithError(Unlocked)
	}
}

type ctxCancel struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type multiLockCtx struct {
	locks    []ctxCancel
	keys     []string
	deadline time.Time
	err      atomic.Value
	once     sync.Once
	done     chan struct{}
}

func newMultiCtx(keys []string, locks []ctxCancel) *multiLockCtx {
	m := &multiLockCtx{
		locks: locks,
		done:  make(chan struct{}),
	}

	for _, lock := range locks {
		if deadline, ok := lock.ctx.Deadline(); ok {
			if m.deadline.IsZero() || deadline.Before(m.deadline) {
				m.deadline = deadline
			}
		}
	}

	go m.run()
	return m
}

func (m *multiLockCtx) Deadline() (time.Time, bool) {
	return m.deadline, !m.deadline.IsZero()
}

func (m *multiLockCtx) Done() <-chan struct{} {
	return m.done
}

func (m *multiLockCtx) Err() error {
	err, _ := m.err.Load().(error)
	return err
}

func (m *multiLockCtx) Value(key interface{}) interface{} {
	if key == LocksKey {
		return copyKeys(m.keys)
	}
	for _, lock := range m.locks {
		if value := lock.ctx.Value(key); value != nil {
			return value
		}
	}
	return nil
}

func (m *multiLockCtx) cancel() {
	m.cancelWithError(context.Canceled)
}

func (m *multiLockCtx) cancelWithError(err error) {
	m.once.Do(func() {
		m.err.Store(err)
		close(m.done)

		wg := sync.WaitGroup{}
		wg.Add(len(m.locks))

		for _, lock := range m.locks {
			go func(cancel context.CancelFunc) {
				cancel()
				wg.Done()
			}(lock.cancel)
		}

		wg.Wait()
	})
}

func (m *multiLockCtx) run() {
	cases := make([]reflect.SelectCase, 0, len(m.locks))

	for _, lock := range m.locks {
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(lock.ctx.Done()),
		})
	}

	n, _, _ := reflect.Select(cases)
	m.cancelWithError(m.locks[n].ctx.Err())
}

func (c *Client) acquireLock(ctx context.Context, key string, sid string) (locked bool, err error) {
	err = c.Put(ctx, "/v1/kv/"+key, Query{{"acquire", sid}}, nil, &locked)
	return
}

func (c *Client) releaseLock(ctx context.Context, key string, sid string) (err error) {
	err = c.Put(ctx, "/v1/kv/"+key, Query{{"release", sid}}, nil, nil)
	return
}

func copyKeys(s []string) []string {
	c := make([]string, len(s))
	copy(c, s)
	return c
}

func sortedKeys(s []string) []string {
	s = copyKeys(s)
	sort.Strings(s)
	return s
}
