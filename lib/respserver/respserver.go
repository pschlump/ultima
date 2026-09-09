// Package respserver wires the vendored redcon fork (lib/resp, design
// doc §6.1) to the command engine (decision D3): one accept/handler/
// closed closure set bridging resp.Conn to commands.Engine, with one
// commands.ConnState per connection. It exists so the RESP front-end
// wiring lives in exactly one place — cmd/ultima-server and both test
// harnesses build their server through New.
//
// Pub/sub (M3): a connection's first subscription lazily starts a push
// writer — a buffered queue drained by its own goroutine through
// conn.PushValue. From then on EVERYTHING written to the connection
// (async broker pushes, command replies, multi-frame subscribe acks)
// flows through that one queue, so acks and messages reach the client in
// exact issue order (single writer). A full queue means a slow consumer;
// the connection is closed, the analogue of Redis's
// client-output-buffer-limit for pub/sub clients.
package respserver

import (
	"sync"

	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/resp"
)

// pushQueueCap bounds a subscribed connection's outbound queue; beyond
// it the consumer is deemed too slow and the connection is closed.
const pushQueueCap = 4096

// pushItem is one queued write; close marks QUIT: the connection is
// closed only after every frame ahead of it (including the +OK reply)
// has been written.
type pushItem struct {
	v     resp.Value
	close bool
}

// pushWriter serializes all writes to a subscribed connection through
// one goroutine.
type pushWriter struct {
	conn resp.Conn
	ch   chan pushItem
	done chan struct{}
	once sync.Once
}

func newPushWriter(conn resp.Conn) *pushWriter {
	pw := &pushWriter{
		conn: conn,
		ch:   make(chan pushItem, pushQueueCap),
		done: make(chan struct{}),
	}
	go pw.loop()
	return pw
}

func (pw *pushWriter) loop() {
	for {
		select {
		case it := <-pw.ch:
			if it.close {
				_ = pw.conn.Close()
				return
			}
			if err := pw.conn.PushValue(it.v); err != nil {
				return // broken connection; the closed callback stops us
			}
		case <-pw.done:
			return
		}
	}
}

// enqueue is the Deliver hook registered with the command engine and the
// broker: a non-blocking send, safe to call after stop (a broker Publish
// snapshot may outlive the connection's close).
func (pw *pushWriter) enqueue(v resp.Value) {
	select {
	case pw.ch <- pushItem{v: v}:
	case <-pw.done:
	default:
		// Slow consumer: drop the connection (Redis pub/sub
		// client-output-buffer-limit analogue).
		_ = pw.conn.Close()
	}
}

// closeEnq queues connection closure behind all pending frames (QUIT in
// push mode); on a full queue it closes directly, matching the
// slow-consumer policy.
func (pw *pushWriter) closeEnq() {
	select {
	case pw.ch <- pushItem{close: true}:
	case <-pw.done:
	default:
		_ = pw.conn.Close()
	}
}

func (pw *pushWriter) stop() {
	pw.once.Do(func() { close(pw.done) })
}

// New returns a *resp.Server on addr whose connections execute against
// eng. The returned server binds nothing; the caller creates the
// listener and calls Serve/Close as usual.
//
// Wiring: accept attaches a fresh ConnState with its StartPush hook (the
// handler re-creates both lazily if absent, e.g. a conn that never went
// through accept); handler runs Engine.Execute, syncs the negotiated
// RESP version back to the connection, and writes the reply plus any
// drained outbox frames — through the push queue once push mode is
// active, directly otherwise; closed stops the push writer and
// deregisters the ConnState.
func New(addr string, eng *commands.Engine) *resp.Server {
	// writers maps a connection to its lazily-created push writer, so
	// the closed callback can stop it.
	var writers sync.Map // resp.Conn -> *pushWriter

	attach := func(conn resp.Conn, cs *commands.ConnState) {
		cs.SetSurface("resp") // M6c client introspection (§10.1)
		cs.SetKillFunc(func() { _ = conn.Close() })
		cs.StartPush = func() func(resp.Value) {
			// Called only from the connection's own goroutine (inside
			// Execute), so load-then-store needs no CAS.
			if pw, ok := writers.Load(conn); ok {
				return pw.(*pushWriter).enqueue
			}
			pw := newPushWriter(conn)
			writers.Store(conn, pw)
			return pw.enqueue
		}
		conn.SetContext(cs)
	}
	handler := func(conn resp.Conn, cmd resp.Command) {
		if len(cmd.Args) == 0 {
			return
		}
		cs, _ := conn.Context().(*commands.ConnState)
		if cs == nil {
			cs = eng.NewConnState(conn.RemoteAddr())
			attach(conn, cs)
		}
		v := eng.Execute(cs, cmd.Args)
		if cs.Proto != conn.ProtocolVersion() {
			conn.SetProtocolVersion(cs.Proto)
		}
		outbox := cs.DrainOutbox()
		if deliver := cs.DeliverFunc(); deliver != nil {
			// Push mode: route everything through the queue so replies
			// and async pushes keep strict issue order.
			deliver(v)
			for _, o := range outbox {
				deliver(o)
			}
		} else {
			conn.WriteValue(v)
			for _, o := range outbox {
				conn.WriteValue(o)
			}
		}
		if cs.Quit {
			if pw, ok := writers.Load(conn); ok {
				// Push mode: close only after the +OK (and everything
				// ahead of it) has been written.
				pw.(*pushWriter).closeEnq()
			} else {
				_ = conn.Close()
			}
		}
	}
	accept := func(conn resp.Conn) bool {
		attach(conn, eng.NewConnState(conn.RemoteAddr()))
		return true
	}
	closed := func(conn resp.Conn, _ error) {
		if pw, ok := writers.LoadAndDelete(conn); ok {
			pw.(*pushWriter).stop()
		}
		if cs, ok := conn.Context().(*commands.ConnState); ok {
			eng.CloseConn(cs)
		}
	}
	return resp.NewServer(addr, handler, accept, closed)
}
