// Package respserver wires the vendored redcon fork (lib/resp, design
// doc §6.1) to the command engine (decision D3): one accept/handler/
// closed closure set bridging resp.Conn to commands.Engine, with one
// commands.ConnState per connection. It exists so the RESP front-end
// wiring lives in exactly one place — cmd/ultima-server and both test
// harnesses build their server through New.
//
// Pub/sub (M3): a connection's first subscription lazily starts a push
// writer — a growable, byte-capped queue (lib/pushq, M9c) drained by its
// own goroutine through conn.PushValue. From then on EVERYTHING written
// to the connection (async broker pushes, command replies, multi-frame
// subscribe acks) flows through that one queue, so acks and messages
// reach the client in exact issue order (single writer). A queue past its
// byte ceiling means a slow consumer; the connection is closed, the
// analogue of Redis's client-output-buffer-limit for pub/sub clients.
package respserver

import (
	"sync"

	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/pushq"
	"github.com/pschlump/ultima/lib/resp"
)

// pushFrameOverhead is the flat per-value-node cost in the byte estimate
// of every queued frame (RESP wire framing: type byte, length header,
// CRLFs, the slice header). Estimates are monotone, not exact — the same
// philosophy as the lib/types memory estimators (D10).
const pushFrameOverhead = 32

// respValueBytes estimates the queued cost of one push frame: payload
// bytes of every node in the value tree plus per-node overhead. The
// broker-built resp.Value is shared across deliveries, never copied.
func respValueBytes(v resp.Value) int {
	n := pushFrameOverhead + len(v.Str) + len(v.Blob)
	for i := range v.Arr {
		n += respValueBytes(v.Arr[i])
	}
	return n
}

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
	q    *pushq.Q[pushItem]
	done chan struct{}
	once sync.Once
}

func newPushWriter(conn resp.Conn, pushBufBytes int) *pushWriter {
	pw := &pushWriter{
		conn: conn,
		q:    pushq.New[pushItem](pushBufBytes),
		done: make(chan struct{}),
	}
	go pw.loop()
	return pw
}

// pushBatchMax bounds one drain-and-flush round: large enough that a busy
// fan-out amortizes the flush syscall over many messages, small enough
// that a queued QUIT close marker is never delayed long.
const pushBatchMax = 1024

func (pw *pushWriter) loop() {
	var batch []resp.Value
	for {
		it, ok := pw.q.Dequeue(pw.done)
		if !ok {
			return
		}
		// M9c: drain everything immediately available and flush once —
		// per-message PushValue flushed per message, and the write
		// syscall was the fan-out bottleneck (8 subscribers × 36K msg/s
		// each → batching lifts per-subscriber throughput ~20×).
		batch = batch[:0]
		for {
			if it.close {
				if len(batch) > 0 {
					if err := pw.conn.PushValues(batch); err != nil {
						return
					}
				}
				_ = pw.conn.Close()
				return
			}
			batch = append(batch, it.v)
			if len(batch) >= pushBatchMax {
				break
			}
			var ok2 bool
			it, ok2 = pw.q.TryDequeue()
			if !ok2 {
				break
			}
		}
		if err := pw.conn.PushValues(batch); err != nil {
			return // broken connection; the closed callback stops us
		}
	}
}

// enqueue is the Deliver hook registered with the command engine and the
// broker: a non-blocking enqueue, safe to call after stop (a broker
// Publish snapshot may outlive the connection's close — the queue fails
// fast once closed). A false return means a slow consumer: drop the
// connection (Redis pub/sub client-output-buffer-limit analogue). The
// drop is a HARD close: a graceful Close flushes under the connection's
// write mutex, which a publisher goroutine can wait on forever while the
// push writer is itself blocked writing to the dead client's socket —
// the pre-M9c slow-consumer deadlock.
func (pw *pushWriter) enqueue(v resp.Value) {
	if !pw.q.Enqueue(pushItem{v: v}, respValueBytes(v)) {
		_ = pw.conn.CloseHard()
	}
}

// closeEnq queues connection closure behind all pending frames (QUIT in
// push mode); the close marker carries no byte cost, so it only fails on
// an already-overflowed or closed queue — closing directly then matches
// the slow-consumer policy (hard close, see enqueue).
func (pw *pushWriter) closeEnq() {
	if !pw.q.Enqueue(pushItem{close: true}, 0) {
		_ = pw.conn.CloseHard()
	}
}

func (pw *pushWriter) stop() {
	pw.once.Do(func() {
		close(pw.done)
		pw.q.Close()
	})
}

// New returns a *resp.Server on addr whose connections execute against
// eng. pushBufBytes caps each subscribed connection's push queue in bytes
// (M9c, server.pubsub_client_buffer_mb); 0 resolves to the lib/pushq
// default. The returned server binds nothing; the caller creates the
// listener and calls Serve/Close as usual.
//
// Wiring: accept attaches a fresh ConnState with its StartPush hook (the
// handler re-creates both lazily if absent, e.g. a conn that never went
// through accept); handler runs Engine.Execute, syncs the negotiated
// RESP version back to the connection, and writes the reply plus any
// drained outbox frames — through the push queue once push mode is
// active, directly otherwise; closed stops the push writer and
// deregisters the ConnState.
func New(addr string, eng *commands.Engine, pushBufBytes int) *resp.Server {
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
			pw := newPushWriter(conn, pushBufBytes)
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
		// M9b burst coalescing: when the read burst holds more pipelined
		// commands, ExecuteBurst runs them all (grouping same-shard runs
		// of single-key commands into one shard task) and the fork's loop
		// drops the consumed remainder. vals carries every reply (plus any
		// outbox frames) in issue order.
		var vals []resp.Value
		if rest := conn.PeekPipeline(); len(rest) > 0 {
			var n int
			vals, n = eng.ExecuteBurst(cs, cmd.Args, rest)
			if n > 0 {
				conn.ReadPipeline()
			}
		} else {
			vals = []resp.Value{eng.Execute(cs, cmd.Args)}
		}
		if cs.Proto != conn.ProtocolVersion() {
			conn.SetProtocolVersion(cs.Proto)
		}
		outbox := cs.DrainOutbox()
		if deliver := cs.DeliverFunc(); deliver != nil {
			// Push mode: route everything through the queue so replies
			// and async pushes keep strict issue order.
			for _, v := range vals {
				deliver(v)
			}
			for _, o := range outbox {
				deliver(o)
			}
		} else {
			for _, v := range vals {
				conn.WriteValue(v)
			}
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
