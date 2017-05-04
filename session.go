package consul

import "context"

// A SessionID is a unique identifier for a session.
type SessionID string

// SessionBehavior is an enumeration of the various bahaviors that can be set to
// configure what happens to keys that are locked by sessions that expire.
type SessionBehavior string

const (
	// Release describes the release behavior, locks are released on keys
	// associated with an expired session.
	Release SessionBehavior = "release"

	// Delete describes the delete behavior, keys are deleted when the session
	// that hold a lock on them expires.
	Delete SessionBehavior = "delete"
)

// SessionConfig is used to configure new sessions.
type SessionConfig struct {
	LockDelay Seconds         `json:",omitempty"`
	Name      string          `json:",omitempty"`
	Node      string          `json:",omitempty"`
	Checks    []string        `json:",omitempty"`
	Behavior  SessionBehavior `json:",omitempty"`
	TTL       Seconds         `json:",omitempty"`
}

// Session carries the metadata associated with a session.
type Session struct {
	LockDelay   float64
	Checks      []string
	Node        string
	ID          SessionID
	CreateIndex uint64
}

// WithSession returns a client which uses a new session created with config.
func (c *Client) WithSession(ctx context.Context, config SessionConfig) (sc *Client, err error) {
	var sid SessionID
	if sid, err = c.CreateSession(ctx, config); err == nil {
		sc = &Client{}
		*sc = *c
		sc.Session = sid
	}
	return
}

// CreateSession creates a new session configured with config and returns its
// id.
func (c *Client) CreateSession(ctx context.Context, config SessionConfig) (sid SessionID, err error) {
	var session struct{ ID SessionID }
	err = c.Put(ctx, "/v1/session/create", nil, config, &session)
	sid = session.ID
	return
}

// DeleteSession deletes the session identified by sid.
func (c *Client) DeleteSession(ctx context.Context, sid SessionID) (err error) {
	err = c.Put(ctx, "/v1/session/destroy/"+string(sid), nil, nil, nil)
	return
}

// ReadSession returns the information of the session identified by sid.
func (c *Client) ReadSession(ctx context.Context, sid SessionID) (s []Session, err error) {
	err = c.Get(ctx, "/v1/session/info/"+string(sid), nil, &s)
	return
}

// ListSessionsForNode returns the active sessions for a give node.
func (c *Client) ListSessionsForNode(ctx context.Context, node string) (s []Session, err error) {
	err = c.Get(ctx, "/v1/session/node/"+node, nil, &s)
	return
}

// ListSessions returns a list of existing sessions.
func (c *Client) ListSessions(ctx context.Context) (s []Session, err error) {
	err = c.Get(ctx, "/v1/session/list", nil, &s)
	return
}

// RenewSession extends a TTL-based session.
func (c *Client) RenewSession(ctx context.Context, sid SessionID) (err error) {
	err = c.Put(ctx, "/v1/session/renew/"+string(sid), nil, nil, nil)
	return
}

// RenewClientSession calls RenewSession on the client's session.
func (c *Client) RenewClientSession(ctx context.Context) (err error) {
	if err = c.checkSession("RenewClientSession"); err == nil {
		err = c.RenewSession(ctx, c.Session)
	}
	return
}
