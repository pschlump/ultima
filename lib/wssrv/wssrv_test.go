package wssrv

// Package-local tests for the WebSocket front-end frame contract
// (design doc §6.3): binary protobuf Command frames, seq-correlated
// replies, pub/sub pushes as unsolicited seq-0 frames, error replies for
// malformed frames, and the shared slow-consumer rule — a full outbound
// queue closes the connection, as in lib/respserver.

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/proto"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/shard"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// newTestServer boots the /ws/v1 handler on an httptest server over a
// small shard engine and returns the engine and the ws:// URL.
func newTestServer(t *testing.T) (*commands.Engine, string) {
	t.Helper()
	shards := shard.NewEngine(4, 16)
	t.Cleanup(shards.Close)
	eng := commands.NewEngine(shards, "test", 0)
	mux := http.NewServeMux()
	mux.HandleFunc("/ws/v1", Handler(eng, slog.New(slog.NewTextHandler(io.Discard, nil))))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return eng, "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/v1"
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func sendCmd(t *testing.T, c *websocket.Conn, cmd *ultimav1.Command) {
	t.Helper()
	b, err := proto.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.WriteMessage(websocket.BinaryMessage, b); err != nil {
		t.Fatal(err)
	}
}

func recv(t *testing.T, c *websocket.Conn) *ultimav1.CommandResponse {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	mt, payload, err := c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if mt != websocket.BinaryMessage {
		t.Fatalf("reply frame type = %d, want binary", mt)
	}
	r := &ultimav1.CommandResponse{}
	if err := proto.Unmarshal(payload, r); err != nil {
		t.Fatal(err)
	}
	return r
}

func generic(seq uint64, name string, args ...string) *ultimav1.Command {
	g := &ultimav1.CommandRequest{Command: name}
	for _, s := range args {
		g.Args = append(g.Args, []byte(s))
	}
	return &ultimav1.Command{Seq: seq, Cmd: &ultimav1.Command_Generic{Generic: g}}
}

// TestSeqCorrelatedReplies: every reply echoes its command's seq, for both
// the typed envelope and the generic escape hatch.
func TestSeqCorrelatedReplies(t *testing.T) {
	_, url := newTestServer(t)
	c := dial(t, url)

	sendCmd(t, c, &ultimav1.Command{Seq: 41, Cmd: &ultimav1.Command_Set{Set: &ultimav1.SetCommand{
		Key: []byte("ws:k"), Value: []byte("v1")}}})
	sendCmd(t, c, generic(42, "get", "ws:k"))
	sendCmd(t, c, generic(43, "ping"))

	set := recv(t, c)
	if set.Seq != 41 || set.Reply.GetSimpleString() != "OK" {
		t.Errorf("SET reply = seq %d %v, want seq 41 OK", set.Seq, set.Reply)
	}
	get := recv(t, c)
	if get.Seq != 42 || string(get.Reply.GetBlobString()) != "v1" {
		t.Errorf("GET reply = seq %d %v, want seq 42 v1", get.Seq, get.Reply)
	}
	ping := recv(t, c)
	if ping.Seq != 43 || ping.Reply.GetSimpleString() != "PONG" {
		t.Errorf("PING reply = seq %d %v, want seq 43 PONG", ping.Seq, ping.Reply)
	}
}

// TestPubSubPush: SUBSCRIBE via the generic envelope returns its ack as
// the seq-correlated reply; broker deliveries then arrive as unsolicited
// seq-0 push frames, and ordinary commands keep working between pushes.
func TestPubSubPush(t *testing.T) {
	eng, url := newTestServer(t)
	sub := dial(t, url)

	sendCmd(t, sub, generic(1, "subscribe", "ws-news"))
	ack := recv(t, sub)
	if ack.Seq != 1 {
		t.Fatalf("subscribe ack seq = %d, want 1", ack.Seq)
	}
	elems := ack.Reply.GetPush().GetElems()
	if len(elems) != 3 || string(elems[0].GetBlobString()) != "subscribe" {
		t.Fatalf("subscribe ack = %v, want [subscribe ws-news 1]", ack.Reply)
	}

	if n := eng.PubSub.Publish("ws-news", []byte("hello-ws"), 0, nil); n != 1 {
		t.Fatalf("broker publish delivered to %d subscribers, want 1", n)
	}
	push := recv(t, sub)
	if push.Seq != 0 {
		t.Errorf("push seq = %d, want 0 (unsolicited)", push.Seq)
	}
	pe := push.Reply.GetPush().GetElems()
	if len(pe) != 3 ||
		string(pe[0].GetBlobString()) != "message" ||
		string(pe[1].GetBlobString()) != "ws-news" ||
		string(pe[2].GetBlobString()) != "hello-ws" {
		t.Errorf("push frame = %v, want [message ws-news hello-ws]", push.Reply)
	}

	// Replies still flow after push mode started, interleaved with pushes.
	sendCmd(t, sub, generic(9, "ping"))
	if r := recv(t, sub); r.Seq != 9 || r.Reply.GetSimpleString() != "PONG" {
		t.Errorf("PING in push mode = seq %d %v, want seq 9 PONG", r.Seq, r.Reply)
	}
}

// TestMalformedFrame: text frames and non-protobuf binary frames get an
// error reply, not a dropped connection — and the server must not die.
func TestMalformedFrame(t *testing.T) {
	_, url := newTestServer(t)
	c := dial(t, url)

	if err := c.WriteMessage(websocket.TextMessage, []byte("PING")); err != nil {
		t.Fatal(err)
	}
	if got := recv(t, c).Reply.GetError(); !strings.HasPrefix(got, "ERR /ws/v1 carries binary") {
		t.Errorf("text frame reply = %q, want binary-only error", got)
	}
	if err := c.WriteMessage(websocket.BinaryMessage, []byte{0xff, 0xff, 0xff}); err != nil {
		t.Fatal(err)
	}
	if got := recv(t, c).Reply.GetError(); !strings.HasPrefix(got, "ERR frame is not a protobuf Command") {
		t.Errorf("garbage frame reply = %q, want unmarshal error", got)
	}
	// The connection survives both.
	sendCmd(t, c, generic(7, "ping"))
	if r := recv(t, c); r.Seq != 7 || r.Reply.GetSimpleString() != "PONG" {
		t.Errorf("PING after bad frames = seq %d %v, want seq 7 PONG", r.Seq, r.Reply)
	}
}

// TestSlowConsumerClosesConnection: a client that stops reading must be
// disconnected once the outbound queue overflows. Flooding publishes far
// past pushQueueCap trips the limit; the client then observes the queued
// frames followed by a terminal read error, within a bounded time.
func TestSlowConsumerClosesConnection(t *testing.T) {
	eng, url := newTestServer(t)
	sub := dial(t, url)

	sendCmd(t, sub, generic(1, "subscribe", "ws-slow"))
	_ = recv(t, sub) // subscribe ack; from here on the client reads nothing

	// Flood well past the queue cap. Publish's delivery count drops to 0
	// once the closed connection's cleanup deregisters the subscription.
	payload := []byte(strings.Repeat("x", 64))
	const maxPublishes = 64 * pushQueueCap
	closed := false
	published := 0
	for ; published < maxPublishes; published++ {
		if eng.PubSub.Publish("ws-slow", payload, 0, nil) == 0 {
			closed = true
			break
		}
	}
	if !closed {
		// The overflowing enqueue may have closed the connection with the
		// read-loop cleanup still in flight; wait briefly for it.
		deadline := time.Now().Add(5 * time.Second)
		for eng.PubSub.NumChannels() != 0 {
			if time.Now().After(deadline) {
				t.Fatalf("subscription still registered after %d publishes; slow consumer was not dropped", published)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	t.Logf("connection closed after %d publishes (queue cap %d)", published, pushQueueCap)

	// The stalled client reads the frames the queue and kernel buffers
	// held, then a terminal error (the server closes the TCP connection
	// without a WebSocket close frame), never a hang.
	if err := sub.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	frames := 0
	for {
		if _, _, err := sub.ReadMessage(); err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatal("read timed out: server never closed the slow consumer's connection")
			}
			t.Logf("blocked reader observed terminal error after %d frames: %v", frames, err)
			break
		}
		frames++
	}
	if frames == 0 {
		t.Fatal("no queued frames were delivered before the close")
	}
}
