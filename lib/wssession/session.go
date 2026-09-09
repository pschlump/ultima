// Package wssession implements Ultima's resumable WebSocket sessions
// (design doc §9.4, decision D18): a per-session replay buffer that
// survives connection drops so a reconnecting client observes no gap in
// pushed messages (pub/sub deliveries, keyspace notifications).
//
// A session owns a monotonic push_seq counter and a bounded buffer of
// every stamped push (bounded by age — auth.ws_replay_buffer_ms — and by
// count — auth.ws_replay_buffer_max_msgs). The session's deliver func is
// the stable hook the pub/sub broker captures at subscribe time, so
// broker registrations survive reconnects untouched: while a connection
// is attached the funnel stamps, buffers, and enqueues live; while
// detached it stamps and buffers only, and the session's subscriptions
// (and its ConnState) are retained until the retention window expires.
//
// Resume is atomic under the session lock: the previous attachment (if
// any — a takeover) is closed, buffered pushes after the client's
// last_push_seq are replayed through the new sink, and only then is the
// new sink installed for live delivery. Because stamping, buffering, and
// live enqueue all happen under the same lock, the client sees every push
// exactly once and in push_seq order.
//
// Command replies are NOT buffered (§9.4): a connection that dies with
// replies undelivered has those correlation seqs recorded via
// RecordAborted; the next successful Attach returns them so the front-end
// can report ABORTED by seq and the client can retry idempotently.
package wssession

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"sync"
	"time"

	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/resp"
)

// Sink is the live delivery target of an attached connection. Enqueue
// must be non-blocking (it is called under the session lock, from
// publisher goroutines); a false return means the connection's outbound
// queue overflowed and the connection is being torn down — the push is
// safe in the buffer regardless. Close aborts the attached connection
// (used on takeover); it must not block. Token identifies the attachment
// so a stale connection's Detach cannot displace a newer one.
type Sink struct {
	Enqueue func(pushSeq uint64, v resp.Value) bool
	Close   func()
	Token   any
}

// bufferedPush is one stamped push retained for replay.
type bufferedPush struct {
	seq uint64
	at  time.Time
	val resp.Value
}

// Session is one resumable WS session: the retained ConnState, the push
// buffer, and the current attachment.
type Session struct {
	ID   string
	User string // account username the session is bound to ("" when auth off)

	reg *Registry
	cs  *commands.ConnState

	mu      sync.Mutex
	nextSeq uint64 // last stamped push_seq; pushes start at 1
	buf     []bufferedPush
	sink    *Sink
	aborted []uint64
	timer   *time.Timer // retention countdown while detached
	expired bool
}

// Registry is the server-wide set of live sessions.
type Registry struct {
	eng     *commands.Engine
	window  time.Duration
	maxMsgs int
	logger  *slog.Logger

	mu       sync.Mutex
	sessions map[string]*Session
	closed   bool
}

// NewRegistry returns a registry whose sessions retain pushes for window
// and at most maxMsgs frames (§8 auth.ws_replay_buffer_ms /
// ws_replay_buffer_max_msgs). The engine is needed to release the
// retained ConnState when a session finally expires.
func NewRegistry(eng *commands.Engine, window time.Duration, maxMsgs int, logger *slog.Logger) *Registry {
	if window <= 0 {
		window = 30 * time.Second
	}
	if maxMsgs <= 0 {
		maxMsgs = 10000
	}
	return &Registry{
		eng:      eng,
		window:   window,
		maxMsgs:  maxMsgs,
		logger:   logger,
		sessions: map[string]*Session{},
	}
}

// New creates a session bound to user over the connection's (fresh)
// ConnState and registers it. The caller attaches the first sink.
func (r *Registry) New(user string, cs *commands.ConnState) *Session {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	s := &Session{
		ID:   hex.EncodeToString(raw[:]),
		User: user,
		reg:  r,
		cs:   cs,
	}
	r.mu.Lock()
	if !r.closed {
		r.sessions[s.ID] = s
	}
	r.mu.Unlock()
	return s
}

// Lookup returns the live session with the given id, or nil.
func (r *Registry) Lookup(id string) *Session {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sessions[id]
}

// Close expires every session (server shutdown): retention timers are
// stopped and the retained ConnStates released.
func (r *Registry) Close() {
	r.mu.Lock()
	r.closed = true
	all := make([]*Session, 0, len(r.sessions))
	for _, s := range r.sessions {
		all = append(all, s)
	}
	r.sessions = map[string]*Session{}
	r.mu.Unlock()
	for _, s := range all {
		s.expire()
	}
}

// remove drops s from the registry (session expiry).
func (r *Registry) remove(id string) {
	r.mu.Lock()
	delete(r.sessions, id)
	r.mu.Unlock()
}

// ConnState returns the session's retained connection state — the same
// object across reconnects, so its broker subscriptions (which carry the
// session's stable deliver func) never need re-registration.
func (s *Session) ConnState() *commands.ConnState { return s.cs }

// Deliver is the push funnel the front-end installs as
// ConnState.StartPush's product: every broker delivery to this session
// flows through it, on the publisher's goroutine. It stamps the push with
// the next push_seq, appends it to the bounded buffer, and — when a
// connection is attached — enqueues it live, all under the session lock
// so live order matches buffer order.
func (s *Session) Deliver(v resp.Value) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expired {
		return
	}
	s.nextSeq++
	s.buf = append(s.buf, bufferedPush{seq: s.nextSeq, at: time.Now(), val: v})
	s.trimLocked()
	if s.sink != nil {
		s.sink.Enqueue(s.nextSeq, v)
	}
}

// trimLocked drops buffer entries beyond the count cap or older than the
// retention window.
func (s *Session) trimLocked() {
	cutoff := time.Now().Add(-s.reg.window)
	n := 0
	for n < len(s.buf) && (len(s.buf)-n > s.reg.maxMsgs || s.buf[n].at.Before(cutoff)) {
		n++
	}
	if n > 0 {
		s.buf = append([]bufferedPush(nil), s.buf[n:]...)
	}
}

// Attach resumes (or starts) delivery over sink after the client
// reported lastSeq as the highest push_seq it received. When the gap
// check fails — the buffer no longer holds lastSeq+1 — Attach changes
// nothing and returns ok=false; the front-end answers SESSION_EXPIRED
// and the session keeps its existing state (a detached session's expiry
// timer keeps running).
//
// On success, atomically under the session lock: the previous attachment
// is closed (takeover), head runs (the front-end enqueues its handshake
// OK frame there, so it lands ahead of the replay on the ordered queue;
// head may be nil), the buffered pushes after lastSeq are replayed
// through sink.Enqueue, the sink is installed for live delivery, the
// retention timer is stopped, and the aborted reply seqs accumulated
// while detached are returned (and cleared) for the front-end to report
// as ABORTED frames.
func (s *Session) Attach(lastSeq uint64, sink Sink, head func()) (aborted []uint64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expired {
		return nil, false
	}
	s.trimLocked()
	if lastSeq < s.nextSeq {
		oldest := s.nextSeq - uint64(len(s.buf)) + 1 // contiguous by construction
		if len(s.buf) == 0 || lastSeq+1 < oldest {
			return nil, false
		}
	}
	if s.sink != nil && s.sink.Close != nil {
		s.sink.Close()
	}
	s.sink = &sink
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	if head != nil {
		head()
	}
	if lastSeq < s.nextSeq {
		for _, p := range s.buf {
			if p.seq > lastSeq {
				if !sink.Enqueue(p.seq, p.val) {
					// Fresh connection's queue overflowed mid-replay (the
					// buffer can exceed the queue cap); the connection is
					// dying and the unreplayed tail stays buffered for the
					// next resume.
					break
				}
			}
		}
	}
	aborted = s.aborted
	s.aborted = nil
	return aborted, true
}

// Detach ends the attachment identified by token (a stale token — from a
// connection a takeover displaced — is ignored) and arms the retention
// timer: if no attachment happens within the window, the session expires.
// Pushes that arrive while detached are buffered by Deliver.
func (s *Session) Detach(token any) {
	s.mu.Lock()
	if s.expired || s.sink == nil || s.sink.Token != token {
		s.mu.Unlock()
		return
	}
	s.sink = nil
	if s.timer != nil {
		s.timer.Stop()
	}
	s.timer = time.AfterFunc(s.reg.window, s.expire)
	s.mu.Unlock()
}

// RecordAborted notes the correlation seqs of command replies that were
// never delivered (the connection dropped with replies queued or
// overflowing); the next Attach reports them as ABORTED frames (§9.4).
func (s *Session) RecordAborted(seqs ...uint64) {
	if len(seqs) == 0 {
		return
	}
	s.mu.Lock()
	if !s.expired {
		s.aborted = append(s.aborted, seqs...)
	}
	s.mu.Unlock()
}

// expire ends the session: the retained ConnState (and with it the
// broker subscriptions) is released. Runs on the retention timer, on
// Registry.Close, and never under Registry.mu.
func (s *Session) expire() {
	s.mu.Lock()
	if s.expired {
		s.mu.Unlock()
		return
	}
	s.expired = true
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	sink := s.sink
	s.sink = nil
	s.mu.Unlock()

	s.reg.remove(s.ID)
	if sink != nil && sink.Close != nil {
		sink.Close()
	}
	s.reg.eng.CloseConn(s.cs)
	if s.reg.logger != nil {
		s.reg.logger.Debug("ws session expired", "session", s.ID, "user", s.User)
	}
}

// BufferedLen reports the current replay-buffer depth (tests and
// introspection).
func (s *Session) BufferedLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buf)
}
