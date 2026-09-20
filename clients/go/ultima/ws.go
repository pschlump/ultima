// Package ultima WebSocket client for /ws/v1 (design doc §6.3, D15) with the §9.4
// resumable-session protocol (D18): session handshake on connect, push_seq
// tracking, exponential-backoff reconnect, transparent re-subscription,
// ABORTED frames failing the matching pending call, and SESSION_EXPIRED
// surfacing a Gap event. Mirrors web/src/lib/ws.ts (the browser twin).
//
// Wire: one binary protobuf Command frame per command; replies are
// CommandResponse frames correlated by seq. Pub/sub and MONITOR pushes
// arrive as unsolicited seq-0 frames carrying a push Value, stamped with
// push_seq on sessioned connections.
package ultima

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/envelope"
	"github.com/pschlump/ultima/lib/resp"
)

// ErrWSClosed is returned by calls on a closed WSClient.
var ErrWSClosed = errors.New("ultima: ws client closed")

// AbortedError reports a command whose reply was lost when its connection
// dropped (§9.4 ABORTED frame): the command may or may not have executed —
// retry only if idempotent.
type AbortedError struct {
	Text string
}

func (e *AbortedError) Error() string { return e.Text }

// WSOptions configures DialWS.
type WSOptions struct {
	// Addr is host:port of the HTTP/WS surface (the /ws/v1 path is implied).
	Addr string
	// TokenProvider supplies the JWT access token presented as the
	// access_token query parameter at upgrade (§9.3). It is consulted on
	// every (re)connect, so a TokenManager keeps it fresh.
	TokenProvider func(ctx context.Context) (string, error)
	// DisableSessions turns the §9.4 handshake off: the connection behaves
	// as in M4 and subscriptions do not survive a reconnect (the client
	// re-subscribes and fires OnGap after every drop).
	DisableSessions bool
	// OnPush is invoked from the read loop for every unsolicited push
	// frame (pub/sub deliveries, subscribe acks beyond the first, MONITOR
	// lines); pushSeq is the session stamp (0 sessionless). It must be
	// quick — it stalls the read loop.
	OnPush func(v resp.Value, pushSeq uint64)
	// OnGap fires when a session was lost (SESSION_EXPIRED) or a
	// sessionless connection dropped: pushed messages were missed and
	// server-side state (subscriptions) was rebuilt from scratch. The
	// client re-subscribes automatically; the handler should re-read any
	// derived state.
	OnGap func()
	// OnResume fires when a dropped session was resumed with no gap.
	OnResume func()
	// OnStateChange fires with true when the connection becomes usable and
	// false when it drops.
	OnStateChange func(open bool)
	// ReconnectMin/Max bound the exponential backoff between reconnect
	// attempts (defaults 100ms / 5s).
	ReconnectMin time.Duration
	ReconnectMax time.Duration
	// DialTimeout bounds each websocket handshake (default 10s).
	DialTimeout time.Duration
}

// WSClient is a goroutine-safe /ws/v1 client (§6.3). Calls made while the
// connection is down block until the reconnect (and §9.4 handshake)
// completes, the call's context expires, or Close is called.
type WSClient struct {
	binarySurface

	opts WSOptions

	mu          sync.Mutex
	conn        *websocket.Conn // nil while down
	seq         uint64
	pending     map[uint64]chan wsResult
	sessionID   string
	lastPushSeq uint64
	ready       chan struct{} // closed once the connection is usable
	done        bool
	supervising bool
	everUp      bool // at least one successful connect (gap detection)
	// Subscription registry for transparent re-subscription (§9.4):
	// retained server-side across resumes, rebuilt after a gap.
	subs  map[string]struct{}
	psubs map[string]struct{}

	writeMu   sync.Mutex // gorilla conns allow one concurrent writer
	closeCh   chan struct{}
	closeOnce sync.Once
}

type wsResult struct {
	val     resp.Value
	session string // session id on handshake replies (§9.4)
	err     error
}

// DialWS connects to /ws/v1 and performs the §9.4 session handshake
// (unless DisableSessions). The returned client reconnects automatically
// until Close.
func DialWS(opts WSOptions) (*WSClient, error) {
	if opts.Addr == "" {
		return nil, errors.New("ultima: WSOptions.Addr is required")
	}
	if opts.ReconnectMin <= 0 {
		opts.ReconnectMin = 100 * time.Millisecond
	}
	if opts.ReconnectMax <= 0 {
		opts.ReconnectMax = 5 * time.Second
	}
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = 10 * time.Second
	}
	c := &WSClient{
		opts:    opts,
		pending: map[uint64]chan wsResult{},
		ready:   make(chan struct{}),
		subs:    map[string]struct{}{},
		psubs:   map[string]struct{}{},
		closeCh: make(chan struct{}),
	}
	c.execFn = c.execCommand
	if err := c.connectOnce(); err != nil {
		_ = c.Close() // stop the reconnect machinery the teardown may have spawned
		return nil, err
	}
	return c, nil
}

// Close permanently closes the client: no reconnect, pending and blocked
// calls fail with ErrWSClosed.
func (c *WSClient) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.done = true
		conn := c.conn
		c.mu.Unlock()
		close(c.closeCh)
		if conn != nil {
			_ = conn.Close()
		}
		c.failAll(ErrWSClosed)
	})
	return nil
}

// SessionID returns the active §9.4 session id ("" when sessionless).
func (c *WSClient) SessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID
}

// Open reports whether the connection is currently usable.
func (c *WSClient) Open() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn != nil
}

func (c *WSClient) setState(open bool) {
	if h := c.opts.OnStateChange; h != nil {
		h(open)
	}
}

// --- connection lifecycle ----------------------------------------------------

// connectOnce dials, runs the handshake, and marks the client usable. On
// later reconnects it re-subscribes after a gap.
func (c *WSClient) connectOnce() error {
	u := url.URL{Scheme: "ws", Host: c.opts.Addr, Path: "/ws/v1"}
	if c.opts.TokenProvider != nil {
		tok, err := c.opts.TokenProvider(context.Background())
		if err != nil {
			return err
		}
		if tok != "" {
			q := u.Query()
			q.Set("access_token", tok)
			u.RawQuery = q.Encode()
		}
	}
	dialer := websocket.Dialer{HandshakeTimeout: c.opts.DialTimeout}
	conn, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		return err
	}

	dead := make(chan struct{})
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	go c.readLoop(conn, dead)

	resumed := false
	if !c.opts.DisableSessions {
		outcome, err := c.handshake(conn, dead)
		if err != nil {
			_ = conn.Close()
			<-dead // let the read loop own the teardown bookkeeping
			return err
		}
		resumed = outcome
	}
	c.mu.Lock()
	close(c.ready)
	first := !c.everUp
	c.everUp = true
	c.mu.Unlock()
	c.setState(true)
	if resumed {
		if h := c.opts.OnResume; h != nil {
			h()
		}
		return nil
	}
	// Fresh session (or sessionless reconnect): subscriptions were lost —
	// rebuild them now that Exec can run. The SESSION_EXPIRED path already
	// fired OnGap inside the handshake; a sessionless reconnect fires it
	// here when there was subscription state to lose.
	lost := c.resubscribe()
	if lost && !first && c.opts.DisableSessions && c.opts.OnGap != nil {
		c.opts.OnGap()
	}
	return nil
}

// handshake performs the §9.4 negotiation on a fresh connection: resume
// the stored session when one exists, else request a fresh one (an empty
// Command frame — proto3 cannot distinguish an unset session field, so
// the empty form is the fresh-session request; see lib/wssrv.isHandshake).
// Reports whether a previous session was resumed without gap.
func (c *WSClient) handshake(conn *websocket.Conn, dead chan struct{}) (resumed bool, err error) {
	for {
		c.mu.Lock()
		sessID, last := c.sessionID, c.lastPushSeq
		c.seq++
		seq := c.seq
		ch := make(chan wsResult, 1)
		c.pending[seq] = ch
		c.mu.Unlock()

		cmd := &ultimav1.Command{Seq: seq, Session: sessID, LastPushSeq: last}
		if err := c.write(conn, cmd); err != nil {
			c.mu.Lock()
			delete(c.pending, seq)
			c.mu.Unlock()
			return false, err
		}
		select {
		case r := <-ch:
			if r.err != nil {
				return false, r.err
			}
			if r.val.Kind == resp.KindError {
				text := r.val.Str
				if strings.HasPrefix(text, "SESSION_EXPIRED") {
					// The session is gone (retention window, replay gap,
					// identity mismatch): the fallback is explicit, never
					// silent (§9.4). Pending commands of the old session can
					// never resolve — fail them — then start fresh.
					c.mu.Lock()
					c.sessionID = ""
					c.lastPushSeq = 0
					c.mu.Unlock()
					c.failAll(errors.New(text))
					if h := c.opts.OnGap; h != nil {
						h()
					}
					continue // fresh handshake on the same connection
				}
				if strings.Contains(text, "resumable sessions are not enabled") {
					return false, nil // run sessionless
				}
				return false, errors.New(text)
			}
			resumed = sessID != "" && sessID == r.session
			c.mu.Lock()
			c.sessionID = r.session
			c.mu.Unlock()
			return resumed, nil
		case <-dead:
			return false, errors.New("ultima: connection lost during session handshake")
		case <-c.closeCh:
			return false, ErrWSClosed
		}
	}
}

// resubscribe replays the tracked (P)SUBSCRIBE set after a gap; reports
// whether there was anything to re-subscribe.
func (c *WSClient) resubscribe() bool {
	c.mu.Lock()
	channels := keysOf(c.subs)
	patterns := keysOf(c.psubs)
	c.mu.Unlock()
	if len(channels) > 0 {
		// Best effort: a failure here means the connection is dropping
		// again; the next reconnect re-runs resubscribe.
		_, _ = c.Exec("SUBSCRIBE", channels...)
	}
	if len(patterns) > 0 {
		_, _ = c.Exec("PSUBSCRIBE", patterns...)
	}
	return len(channels)+len(patterns) > 0
}

func keysOf(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// supervise reconnects with exponential backoff until success or Close.
func (c *WSClient) supervise() {
	backoff := c.opts.ReconnectMin
	for {
		select {
		case <-c.closeCh:
			return
		case <-time.After(backoff):
		}
		if err := c.connectOnce(); err == nil {
			return // up; the read loop re-spawns supervise on the next drop
		}
		backoff *= 2
		if backoff > c.opts.ReconnectMax {
			backoff = c.opts.ReconnectMax
		}
	}
}

// readLoop dispatches frames until the connection dies; it then flips the
// client to not-ready and spawns the reconnect supervisor.
func (c *WSClient) readLoop(conn *websocket.Conn, dead chan struct{}) {
	defer close(dead)
	defer func() {
		c.mu.Lock()
		if c.conn != conn || c.done {
			c.mu.Unlock()
			return // superseded (takeover) or closing
		}
		c.conn = nil
		c.ready = make(chan struct{})
		if c.opts.DisableSessions {
			// No §9.4 buffer: in-flight replies are unrecoverable.
			for seq, ch := range c.pending {
				ch <- wsResult{err: errors.New("ultima: connection dropped")}
				delete(c.pending, seq)
			}
		}
		spawn := !c.supervising
		c.supervising = true
		c.mu.Unlock()
		c.setState(false)
		if spawn {
			go func() {
				c.supervise()
				c.mu.Lock()
				c.supervising = false
				c.mu.Unlock()
			}()
		}
	}()

	for {
		mt, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if mt != websocket.BinaryMessage {
			continue
		}
		var r ultimav1.CommandResponse
		if err := proto.Unmarshal(payload, &r); err != nil {
			continue
		}
		c.onFrame(&r)
	}
}

func (c *WSClient) onFrame(r *ultimav1.CommandResponse) {
	if r.GetSeq() == 0 {
		// Unsolicited push frame (§6.3): track the §9.4 stamp for resume.
		ps := r.GetPushSeq()
		c.mu.Lock()
		if ps > c.lastPushSeq {
			c.lastPushSeq = ps
		}
		c.mu.Unlock()
		if h := c.opts.OnPush; h != nil {
			h(envelope.FromProto(r.GetReply()), ps)
		}
		return
	}
	v := envelope.FromProto(r.GetReply())
	c.mu.Lock()
	ch, ok := c.pending[r.GetSeq()]
	if ok {
		delete(c.pending, r.GetSeq())
	}
	c.mu.Unlock()
	if !ok {
		// Multi-channel (P)SUBSCRIBE answers one ack frame per channel, all
		// carrying the command's seq (lib/envelope drains the ConnState
		// outbox with the same seq): only the first resolves the pending
		// call; route the rest to the push handler like RESP does.
		if v.Kind == resp.KindPush && c.opts.OnPush != nil {
			c.opts.OnPush(v, 0)
		}
		return
	}
	// §9.4 ABORTED: the reply to this seq was lost with the previous
	// connection — fail the call explicitly.
	if v.Kind == resp.KindError && strings.HasPrefix(v.Str, "ABORTED") {
		ch <- wsResult{err: &AbortedError{Text: v.Str}}
		return
	}
	ch <- wsResult{val: v, session: r.GetSession()}
}

// write marshals and sends one Command frame.
func (c *WSClient) write(conn *websocket.Conn, cmd *ultimav1.Command) error {
	frame, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return conn.WriteMessage(websocket.BinaryMessage, frame)
}

// failAll fails every pending call (Close, SESSION_EXPIRED).
func (c *WSClient) failAll(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for seq, ch := range c.pending {
		ch <- wsResult{err: err}
		delete(c.pending, seq)
	}
}

// execCommand routes one Command through the connection, waiting out any
// reconnect in progress.
func (c *WSClient) execCommand(ctx context.Context, cmd *ultimav1.Command) (resp.Value, error) {
	c.mu.Lock()
	ready := c.ready
	c.mu.Unlock()
	select {
	case <-ready:
	case <-ctx.Done():
		return resp.Value{}, ctx.Err()
	case <-c.closeCh:
		return resp.Value{}, ErrWSClosed
	}

	// Fetch the connection only after ready closes: connectOnce installs
	// the live conn before marking ready, so this is never nil/stale.
	conn := c.currentConn()
	if conn == nil {
		return resp.Value{}, errors.New("ultima: connection not established")
	}
	c.mu.Lock()
	c.seq++
	cmd.Seq = c.seq
	ch := make(chan wsResult, 1)
	c.pending[cmd.Seq] = ch
	c.mu.Unlock()
	if err := c.write(conn, cmd); err != nil {
		c.mu.Lock()
		delete(c.pending, cmd.GetSeq())
		c.mu.Unlock()
		return resp.Value{}, err
	}
	select {
	case r := <-ch:
		return r.val, r.err
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, cmd.GetSeq())
		c.mu.Unlock()
		return resp.Value{}, ctx.Err()
	case <-c.closeCh:
		return resp.Value{}, ErrWSClosed
	}
}

func (c *WSClient) currentConn() *websocket.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn
}

// Subscribe — SUBSCRIBE channel [channel ...]; deliveries arrive via the
// OnPush handler. The subscription set is tracked so §9.4 recovery can
// re-subscribe after a gap.
func (c *WSClient) Subscribe(channels ...string) (resp.Value, error) {
	v, err := c.Exec("SUBSCRIBE", channels...)
	if err == nil && v.Kind != resp.KindError {
		c.mu.Lock()
		for _, ch := range channels {
			c.subs[ch] = struct{}{}
		}
		c.mu.Unlock()
	}
	return v, err
}

// PSubscribe — PSUBSCRIBE pattern [pattern ...]; see Subscribe.
func (c *WSClient) PSubscribe(patterns ...string) (resp.Value, error) {
	v, err := c.Exec("PSUBSCRIBE", patterns...)
	if err == nil && v.Kind != resp.KindError {
		c.mu.Lock()
		for _, p := range patterns {
			c.psubs[p] = struct{}{}
		}
		c.mu.Unlock()
	}
	return v, err
}

// Unsubscribe — UNSUBSCRIBE [channel ...]; no arguments unsubscribes all.
func (c *WSClient) Unsubscribe(channels ...string) (resp.Value, error) {
	v, err := c.Exec("UNSUBSCRIBE", channels...)
	if err == nil && v.Kind != resp.KindError {
		c.mu.Lock()
		if len(channels) == 0 {
			c.subs = map[string]struct{}{}
		}
		for _, ch := range channels {
			delete(c.subs, ch)
		}
		c.mu.Unlock()
	}
	return v, err
}

// PUnsubscribe — PUNSUBSCRIBE [pattern ...]; see Unsubscribe.
func (c *WSClient) PUnsubscribe(patterns ...string) (resp.Value, error) {
	v, err := c.Exec("PUNSUBSCRIBE", patterns...)
	if err == nil && v.Kind != resp.KindError {
		c.mu.Lock()
		if len(patterns) == 0 {
			c.psubs = map[string]struct{}{}
		}
		for _, p := range patterns {
			delete(c.psubs, p)
		}
		c.mu.Unlock()
	}
	return v, err
}

// Ping — PING over the generic envelope.
func (c *WSClient) Ping() (resp.Value, error) { return c.Exec("PING") }

// String describes the client for diagnostics.
func (c *WSClient) String() string {
	return fmt.Sprintf("ultima ws://%s/ws/v1 (session %q)", c.opts.Addr, c.SessionID())
}
