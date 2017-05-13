package consul

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// SessionID is a type representing unique session identifiers.
type SessionID string

// SessionBehavior is an enumeration repesenting the behaviors of session
// expirations.
type SessionBehavior string

const (
	// Release describes the release behavior, locks are released on keys
	// associated with an expired session.
	Release SessionBehavior = "release"

	// Delete describes the delete behavior, keys are deleted when the session
	// that hold a lock on them expires.
	Delete SessionBehavior = "delete"
)

// Session carries a session configuration, it is used in the WithSession
// function to customize the session properties.
type Session struct {
	// The client used to create the session.
	Client *Client

	// A human-readable name for the session (optional).
	Name string

	// The session ID, this should not be set when creating the session but it
	// may be read from the context associated with the session.
	ID SessionID

	// The behavior to apply to keys associated with the session when the
	// session expires.
	//
	// If unset, uses Release.
	Behavior SessionBehavior

	// LockDelay is the amount of time that a lock will stay held if it hasn't
	// been released and the session that was attached to it expired.
	//
	// If zero, 15 seconds is used.
	LockDelay time.Duration

	// The time-to-live of the session, a session automatically expires if it
	// hasn't been renewed for longer than its TTL.
	//
	// If zero, uses 2 x LockDelay.
	TTL time.Duration
}

var (
	// SessionKey is the key at which the Session value is stored in a context.
	SessionKey = &contextKey{"consul-session"}
)

// WithSession constructs a copy of the context which is attached to a newly
// created session.
func WithSession(ctx context.Context, session Session) (context.Context, context.CancelFunc) {
	if session.Client == nil {
		session.Client = DefaultClient
	}

	if len(session.Behavior) == 0 {
		session.Behavior = Release
	}

	if session.LockDelay == 0 {
		session.LockDelay = 15 * time.Second
	}

	if session.TTL == 0 {
		session.TTL = 2 * session.LockDelay
	}

	createSessionCtx, createSessionCancel := context.WithTimeout(ctx, session.LockDelay)
	defer createSessionCancel()

	sid, err := session.Client.createSession(createSessionCtx, sessionConfig{
		Name:      session.Name,
		Behavior:  string(session.Behavior),
		LockDelay: seconds(session.LockDelay),
		TTL:       seconds(session.TTL),
	})

	if err != nil {
		return errorContext(ctx, err)
	}

	session.ID = SessionID(sid)
	sessionCtx := newSessionCtx(ctx, session)
	return sessionCtx, sessionCtx.cancel
}

type sessionConfig struct {
	Name      string   `json:",omitempty"`
	Node      string   `json:",omitempty"`
	Checks    []string `json:",omitempty"`
	Behavior  string   `json:",omitempty"`
	LockDelay string   `json:",omitempty"`
	TTL       string   `json:",omitempty"`
}

func (c *Client) createSession(ctx context.Context, config sessionConfig) (sid string, err error) {
	var session struct{ ID string }
	err = c.Put(ctx, "/v1/session/create", nil, config, &session)
	sid = session.ID
	return
}

func (c *Client) destroySession(ctx context.Context, sid string) (err error) {
	err = c.Put(ctx, "/v1/session/destroy/"+sid, nil, nil, nil)
	return
}

func (c *Client) renewSession(ctx context.Context, sid string) (err error) {
	err = c.Put(ctx, "/v1/session/renew/"+sid, nil, nil, nil)
	return
}

type sessionCtx struct {
	session Session
	ctx     context.Context
	err     atomic.Value
	once    sync.Once
	done    chan struct{}
}

func newSessionCtx(ctx context.Context, session Session) *sessionCtx {
	s := &sessionCtx{
		session: session,
		ctx:     ctx,
		done:    make(chan struct{}),
	}
	go s.run()
	return s
}

func (s *sessionCtx) Deadline() (time.Time, bool) {
	return s.ctx.Deadline()
}

func (s *sessionCtx) Done() <-chan struct{} {
	return s.done
}

func (s *sessionCtx) Err() error {
	err, _ := s.err.Load().(error)
	return err
}

func (s *sessionCtx) Value(key interface{}) interface{} {
	if key == SessionKey {
		return s.session
	}
	return s.ctx.Value(key)
}

func (s *sessionCtx) cancel() {
	s.cancelWithError(context.Canceled)
}

func (s *sessionCtx) cancelWithError(err error) {
	s.once.Do(func() {
		s.err.Store(err)
		close(s.done)

		ctx, cancel := context.WithTimeout(context.Background(), s.session.LockDelay)
		s.session.Client.destroySession(ctx, s.id())
		cancel()
	})
}

func (s *sessionCtx) id() string {
	return string(s.session.ID)
}

func (s *sessionCtx) run() {
	timeout := s.session.TTL / 2
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case <-s.done:
			return
		case <-s.ctx.Done():
			s.cancelWithError(s.ctx.Err())
			return
		}

		// TODO: tolerate failures here? add retries with backoff?
		renewSessionCtx, renewSessionCancel := context.WithTimeout(s, timeout)
		err := s.session.Client.renewSession(renewSessionCtx, s.id())
		renewSessionCancel()

		if err != nil {
			s.cancelWithError(err)
			return
		}
	}
}

func contextSession(ctx context.Context) Session {
	return ctx.Value(SessionKey).(Session)
}
