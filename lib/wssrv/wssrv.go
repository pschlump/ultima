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
// connection. JWT auth at upgrade and resumable sessions (§9.3/§9.4)
// arrive with M6; until then the endpoint accepts any origin, as the M0
// stub did.
package wssrv

import (
	"log/slog"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/envelope"
	"github.com/pschlump/ultima/lib/resp"
)

// pushQueueCap bounds a connection's outbound frame queue (replies plus
// pub/sub pushes); beyond it the client is a slow consumer and the
// connection is closed, matching lib/respserver.
const pushQueueCap = 4096

// Handler returns the /ws/v1 handler wired to the shared command engine.
func Handler(eng *commands.Engine, logger *slog.Logger) http.HandlerFunc {
	upgrader := websocket.Upgrader{
		// No origin policy until the auth milestone (M6) lands.
		CheckOrigin: func(*http.Request) bool { return true },
	}
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logger.Warn("ws: upgrade failed", "err", err)
			return
		}
		serve(eng, conn, r.RemoteAddr)
	}
}

// wsConn serializes all writes to the websocket through one writer
// goroutine fed by a bounded queue; replies (from the read loop) and
// broker pushes (from publisher goroutines) both enqueue here.
type wsConn struct {
	conn *websocket.Conn
	send chan []byte
	done chan struct{}
	once sync.Once
}

func serve(eng *commands.Engine, conn *websocket.Conn, addr string) {
	ws := &wsConn{
		conn: conn,
		send: make(chan []byte, pushQueueCap),
		done: make(chan struct{}),
	}
	defer ws.close()
	go ws.writer()

	cs := eng.NewConnState(addr)
	cs.Proto = 3 // binary clients get full RESP3-grade fidelity
	cs.StartPush = func() func(resp.Value) {
		return func(v resp.Value) {
			// Unsolicited push: seq 0, RESP3 push value (§6.3).
			ws.enqueue(&ultimav1.CommandResponse{Reply: envelope.ToProto(v)})
		}
	}
	defer eng.CloseConn(cs)

	for {
		mt, payload, err := conn.ReadMessage()
		if err != nil {
			return // client went away or sent a close frame
		}
		if mt != websocket.BinaryMessage {
			ws.enqueue(errorFrame("ERR /ws/v1 carries binary protobuf Command frames (design doc §6.3)"))
			continue
		}
		var cmd ultimav1.Command
		if err := proto.Unmarshal(payload, &cmd); err != nil {
			ws.enqueue(errorFrame("ERR frame is not a protobuf Command: " + err.Error()))
			continue
		}
		for _, r := range envelope.Execute(eng, cs, &cmd) {
			if !ws.enqueue(r) {
				return
			}
		}
		if cs.Quit {
			return
		}
	}
}

// enqueue marshals r and queues the frame; a full queue marks the client a
// slow consumer and tears the connection down. Reports whether the frame
// was queued.
func (ws *wsConn) enqueue(r *ultimav1.CommandResponse) bool {
	b, err := proto.Marshal(r)
	if err != nil {
		return true // cannot happen for these messages; drop rather than die
	}
	select {
	case ws.send <- b:
		return true
	default:
		ws.close()
		return false
	}
}

func (ws *wsConn) writer() {
	for {
		select {
		case b := <-ws.send:
			if err := ws.conn.WriteMessage(websocket.BinaryMessage, b); err != nil {
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

func errorFrame(msg string) *ultimav1.CommandResponse {
	return &ultimav1.CommandResponse{Reply: envelope.ToProto(resp.Err(msg))}
}
