// Package wssrv hosts Ultima's WebSocket front-end (design doc §6.3): one
// binary protobuf frame per Command, replies as CommandResponse frames
// correlated by seq — the same envelope as the gRPC stream (decision D15),
// over a browser-friendly transport with no RESP parsing. The M0 text
// PING/PONG stub is retired; text frames now get an error reply.
//
// Pub/sub pushes (SUBSCRIBE via the generic envelope) are delivered as
// unsolicited CommandResponse frames with seq 0 carrying a RESP3 push
// value — the same slow-consumer rule as the RESP surface applies: the
// per-connection outbound queue is bounded and a full queue closes the
// connection. JWT auth at upgrade (§9.3) landed with M6a — when the auth
// service is enabled the client presents its access token as the
// `access_token` query parameter or as `Sec-WebSocket-Protocol: bearer,
// <token>` (browsers cannot set arbitrary headers on a WebSocket).
//
// Resumable sessions (§9.4, decision D18) landed with M6b: the first
// frame on a connection may be a handshake (empty Command for a fresh
// session, or session id + last_push_seq to resume). Resumed sessions
// replay buffered pushes after last_push_seq, keep their subscriptions
// across the drop (lib/wssession retains the ConnState), and learn which
// in-flight command replies were lost via ABORTED error frames carrying
// the command's seq. A resume that misses the retention window gets a
// SESSION_EXPIRED error frame — explicit, never silent. The M6d origin
// policy (server.ws_origin_allow) applies when auth is enabled: browser
// upgrades must be same-origin or allowlisted; auth-disabled servers keep
// the pre-M6 no-origin-policy posture.
package wssrv

import (
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/auth"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/envelope"
	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/wssession"
)

// pushQueueCap bounds a connection's outbound frame queue (replies plus
// pub/sub pushes); beyond it the client is a slow consumer and the
// connection is closed, matching lib/respserver.
const pushQueueCap = 4096

// bearerSubprotocol is the WebSocket subprotocol name a client offers to
// carry its access token: `Sec-WebSocket-Protocol: bearer, <token>`.
// When selected, the server echoes `bearer` as the negotiated
// subprotocol.
const bearerSubprotocol = "bearer"

// Handler returns the /ws/v1 handler wired to the shared command engine.
// authSvc is the M6a auth service; nil leaves the endpoint open (the
// pre-M6 behavior, used when auth.enabled is false). reg is the M6b
// resumable-session registry; nil disables the session handshake.
// originAllow is the M6d origin policy (server.ws_origin_allow,
// comma-separated full origins or hosts, "*" for any): it applies only
// when authSvc != nil — a missing Origin header (non-browser clients)
// and same-host origins always pass. With auth disabled the endpoint
// keeps its pre-M6 no-origin-policy posture.
func Handler(eng *commands.Engine, authSvc *auth.Service, reg *wssession.Registry, logger *slog.Logger, originAllow []string) http.HandlerFunc {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			if authSvc == nil {
				return true
			}
			return checkOrigin(r, originAllow)
		},
	}
	if authSvc != nil {
		upgrader.Subprotocols = []string{bearerSubprotocol}
	}
	return func(w http.ResponseWriter, r *http.Request) {
		var id auth.Identity
		if authSvc != nil {
			var err error
			id, err = upgradeIdentity(authSvc, r)
			if err != nil {
				http.Error(w, `{"status":"error","error":"unauthorized"}`, http.StatusUnauthorized)
				return
			}
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logger.Warn("ws: upgrade failed", "err", err)
			return
		}
		serve(eng, reg, conn, r.RemoteAddr, id)
	}
}

// checkOrigin is the M6d origin policy (§10.2): browser upgrades must be
// same-origin or on the configured allowlist. A missing Origin header
// passes — non-browser clients (CLIs, tests) send none, and the bearer
// credential is presented explicitly, so there is no ambient-credential
// CSRF surface to defend there.
func checkOrigin(r *http.Request, allow []string) bool {
	o := r.Header.Get("Origin")
	if o == "" {
		return true
	}
	u, err := url.Parse(o)
	if err != nil || u.Host == "" {
		return false
	}
	if strings.EqualFold(u.Host, r.Host) {
		return true
	}
	for _, a := range allow {
		if a == "*" || strings.EqualFold(a, o) || strings.EqualFold(a, u.Host) {
			return true
		}
	}
	return false
}

// upgradeIdentity extracts and verifies the access token presented at
// upgrade time (§9.3): the access_token query parameter wins, then the
// bearer subprotocol form.
func upgradeIdentity(authSvc *auth.Service, r *http.Request) (auth.Identity, error) {
	tok := r.URL.Query().Get("access_token")
	if tok == "" {
		parts := strings.Split(r.Header.Get("Sec-WebSocket-Protocol"), ",")
		if len(parts) == 2 && strings.TrimSpace(parts[0]) == bearerSubprotocol {
			tok = strings.TrimSpace(parts[1])
		}
	}
	return authSvc.VerifyAccess(tok)
}

// wsFrame is one queued outbound frame plus the bookkeeping the teardown
// path needs: replies that never made it out are reported ABORTED by seq
// on the next resume; pushes need no such tracking because the session
// replay buffer holds them.
type wsFrame struct {
	resp   *ultimav1.CommandResponse
	data   []byte
	seq    uint64
	isPush bool
}

// wsConn serializes all writes to the websocket through one writer
// goroutine fed by a bounded queue; replies (from the read loop) and
// broker pushes (from publisher goroutines) both enqueue here.
type wsConn struct {
	conn *websocket.Conn
	send chan wsFrame
	done chan struct{}
	once sync.Once
	wg   sync.WaitGroup // the writer goroutine

	// onDrop reports the seq of a reply frame the writer failed to write
	// (client gone mid-write) — recorded ABORTED for the session. Set by
	// the read loop when a session attaches; mutex-guarded against the
	// writer goroutine.
	dropMu sync.Mutex
	onDrop func(seq uint64)
}

// setOnDrop installs the write-failure hook (session attach).
func (ws *wsConn) setOnDrop(f func(seq uint64)) {
	ws.dropMu.Lock()
	ws.onDrop = f
	ws.dropMu.Unlock()
}

func serve(eng *commands.Engine, reg *wssession.Registry, conn *websocket.Conn, addr string, id auth.Identity) {
	ws := &wsConn{
		conn: conn,
		send: make(chan wsFrame, pushQueueCap),
		done: make(chan struct{}),
	}
	defer ws.close()
	ws.wg.Add(1)
	go ws.writer()

	cs := eng.NewConnState(addr)
	cs.Proto = 3        // binary clients get full RESP3-grade fidelity
	cs.SetSurface("ws") // M6c client introspection (§10.1)
	// Kill closes the underlying net.Conn, which fails the read loop and
	// runs the normal teardown (sessioned connections detach per §9.4).
	cs.SetKillFunc(func() { _ = conn.Close() })
	if id.Username != "" {
		cs.Authed = true
		cs.User = id.Username
	}
	// Sessionless push funnel (§6.3): unsolicited push, seq 0, no
	// push_seq. A session handshake replaces this with the session's
	// stamping funnel (§9.4).
	cs.StartPush = func() func(resp.Value) {
		return func(v resp.Value) {
			ws.enqueue(&ultimav1.CommandResponse{Reply: envelope.ToProto(v)})
		}
	}

	var sess *wssession.Session
	// Connection teardown. A sessioned connection detaches instead of
	// closing its ConnState: the session retains subscriptions and buffers
	// pushes through the reconnect window (§9.4). Replies still queued (or
	// refused) at teardown are lost with the connection — their seqs are
	// recorded so the next resume reports them ABORTED.
	defer func() {
		ws.close()
		ws.wg.Wait() // writer is done racing the queue drain below
		if sess == nil {
			eng.CloseConn(cs)
			return
		}
		var lost []uint64
		for {
			select {
			case fr := <-ws.send:
				if !fr.isPush {
					lost = append(lost, fr.seq)
				}
			default:
				sess.RecordAborted(lost...)
				sess.Detach(ws)
				return
			}
		}
	}()

	for {
		mt, payload, err := conn.ReadMessage()
		if err != nil {
			return // client went away or sent a close frame
		}
		if mt != websocket.BinaryMessage {
			ws.enqueue(errorFrame(0, "ERR /ws/v1 carries binary protobuf Command frames (design doc §6.3)"))
			continue
		}
		var cmd ultimav1.Command
		if err := proto.Unmarshal(payload, &cmd); err != nil {
			ws.enqueue(errorFrame(0, "ERR frame is not a protobuf Command: "+err.Error()))
			continue
		}
		if isHandshake(&cmd) {
			if sess != nil {
				ws.enqueue(errorFrame(cmd.GetSeq(), "ERR session already negotiated on this connection"))
				continue
			}
			sess, cs = handshake(eng, reg, ws, cs, id, &cmd)
			continue
		}
		for _, r := range envelope.Execute(eng, cs, &cmd) {
			if !ws.enqueue(r) {
				if sess != nil {
					sess.RecordAborted(r.GetSeq())
				}
				return
			}
		}
		if cs.Quit {
			return
		}
	}
}

// isHandshake reports whether a frame is a session-negotiation frame
// (§9.4): any frame carrying session fields, or an empty Command (no cmd
// oneof set), which requests a fresh session. Note proto3 cannot
// distinguish an empty session string from an unset one, so a fresh-
// session request is exactly the empty-Command form.
func isHandshake(cmd *ultimav1.Command) bool {
	return cmd.GetSession() != "" || cmd.GetLastPushSeq() != 0 || cmd.GetCmd() == nil
}

// handshake processes one session-negotiation frame: it creates a fresh
// session (empty session field) or resumes the named one, answering with
// a session-carrying reply — OK, or a SESSION_EXPIRED error when the
// resume cannot be honored (unknown id, expired retention window,
// identity mismatch, or a replay gap). Returns the session and ConnState
// the read loop runs with from here on (both unchanged on failure).
func handshake(eng *commands.Engine, reg *wssession.Registry, ws *wsConn, cs *commands.ConnState, id auth.Identity, cmd *ultimav1.Command) (*wssession.Session, *commands.ConnState) {
	if reg == nil {
		ws.enqueue(errorFrame(cmd.GetSeq(), "ERR resumable sessions are not enabled on this server"))
		return nil, cs
	}

	if name := cmd.GetSession(); name != "" {
		sess := reg.Lookup(name)
		// The session is bound to the account that created it: a resume
		// presenting a different identity is just an expired session.
		if sess == nil || sess.User != id.Username {
			ws.enqueue(sessionExpiredFrame(cmd.GetSeq(), name))
			return nil, cs
		}
		// head enqueues the resume-OK inside the Attach critical section,
		// so it lands on the ordered queue ahead of the replayed pushes.
		head := func() {
			ws.enqueue(&ultimav1.CommandResponse{
				Seq:     cmd.GetSeq(),
				Session: sess.ID,
				Reply:   envelope.ToProto(resp.Simple("OK")),
			})
		}
		aborted, ok := sess.Attach(cmd.GetLastPushSeq(), ws.sink(), head)
		if !ok {
			// The session is untouched (a detached one's expiry timer keeps
			// running); this connection stays sessionless on its fresh
			// ConnState and may hand a fresh handshake in next.
			ws.enqueue(sessionExpiredFrame(cmd.GetSeq(), name))
			return nil, cs
		}
		// The fresh ConnState this connection opened with is discarded —
		// the session's retained one (subscriptions and all) takes over.
		eng.CloseConn(cs)
		cs = sess.ConnState()
		// ABORTED frames follow the replay: one per in-flight command
		// reply the previous incarnation lost.
		for _, seq := range aborted {
			ws.enqueue(errorFrame(seq, "ABORTED connection dropped before this reply was delivered"))
		}
		ws.setOnDrop(func(seq uint64) { sess.RecordAborted(seq) })
		return sess, cs
	}

	sess := reg.New(id.Username, cs)
	cs.StartPush = func() func(resp.Value) { return sess.Deliver }
	if _, ok := sess.Attach(0, ws.sink(), nil); !ok {
		// Registry closed mid-connect (server shutting down).
		ws.enqueue(sessionExpiredFrame(cmd.GetSeq(), ""))
		return nil, cs
	}
	ws.enqueue(&ultimav1.CommandResponse{
		Seq:     cmd.GetSeq(),
		Session: sess.ID,
		Reply:   envelope.ToProto(resp.Simple("OK")),
	})
	ws.setOnDrop(func(seq uint64) { sess.RecordAborted(seq) })
	return sess, cs
}

// sink builds the session delivery target for this connection: stamped
// pushes marshal to seq-0 CommandResponse frames carrying push_seq.
func (ws *wsConn) sink() wssession.Sink {
	return wssession.Sink{
		Enqueue: func(pushSeq uint64, v resp.Value) bool {
			return ws.enqueue(&ultimav1.CommandResponse{
				PushSeq: pushSeq,
				Reply:   envelope.ToProto(v),
			})
		},
		Close: ws.close,
		Token: ws,
	}
}

// enqueue marshals r and queues the frame; a full queue marks the client a
// slow consumer and tears the connection down. Reports whether the frame
// was queued.
func (ws *wsConn) enqueue(r *ultimav1.CommandResponse) bool {
	return ws.enqueueFrame(replyFrame(r))
}

// replyFrame tags a marshaled response as a reply (ABORTED-tracked) or a
// push (replay-buffered), by the same rule the read loop uses: pushes are
// unsolicited seq-0 frames.
func replyFrame(r *ultimav1.CommandResponse) wsFrame {
	return wsFrame{resp: r, isPush: r.GetPushSeq() != 0}
}

// enqueueFrame queues fr; see enqueue.
func (ws *wsConn) enqueueFrame(fr wsFrame) bool {
	b, err := proto.Marshal(fr.resp)
	if err != nil {
		return true // cannot happen for these messages; drop rather than die
	}
	fr.data = b
	fr.seq = fr.resp.GetSeq()
	select {
	case ws.send <- fr:
		return true
	default:
		ws.close()
		return false
	}
}

func (ws *wsConn) writer() {
	defer ws.wg.Done()
	for {
		select {
		case fr := <-ws.send:
			if err := ws.conn.WriteMessage(websocket.BinaryMessage, fr.data); err != nil {
				if !fr.isPush {
					ws.dropMu.Lock()
					if ws.onDrop != nil {
						ws.onDrop(fr.seq)
					}
					ws.dropMu.Unlock()
				}
				ws.close()
				return
			}
		case <-ws.done:
			return
		}
	}
}

func (ws *wsConn) close() {
	ws.once.Do(func() {
		close(ws.done)
		_ = ws.conn.Close()
	})
}

// errorFrame builds a reply frame carrying an error value.
func errorFrame(seq uint64, msg string) *ultimav1.CommandResponse {
	return &ultimav1.CommandResponse{Seq: seq, Reply: envelope.ToProto(resp.Err(msg))}
}

// sessionExpiredFrame builds the SESSION_EXPIRED handshake answer (§9.4):
// the client re-authenticates, re-subscribes, and re-reads state before
// resuming — the fallback is explicit, never silent.
func sessionExpiredFrame(seq uint64, session string) *ultimav1.CommandResponse {
	return &ultimav1.CommandResponse{
		Seq:     seq,
		Session: session,
		Reply:   envelope.ToProto(resp.Err("SESSION_EXPIRED session not found, retention window exceeded, or replay gap")),
	}
}
