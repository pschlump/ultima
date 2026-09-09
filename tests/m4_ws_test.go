// M4 WebSocket front-end tests (design doc §6.3, decision D15): the same
// protobuf Command envelope as the gRPC stream, carried one binary frame
// per command at /ws/v1, with unsolicited seq-0 push frames for pub/sub.
package tests

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/wssession"
	"github.com/pschlump/ultima/lib/wssrv"
)

// wsTestServer boots the WS front-end on an httptest server and returns a
// dialer for /ws/v1.
func wsTestServer(t *testing.T) (*commands.Engine, string) {
	t.Helper()
	shards := shard.NewEngine(0, 16)
	t.Cleanup(shards.Close)
	eng := commands.NewEngine(shards, "test", 0)
	reg := wssession.NewRegistry(eng, 0, 0, testLogger())
	t.Cleanup(reg.Close)

	r := chi.NewRouter()
	r.Get("/ws/v1", wssrv.Handler(eng, nil, reg, testLogger()))
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return eng, "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/v1"
}

func wsDial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// wsRoundTrip sends the commands (one binary frame each) and reads one
// reply per command.
func wsRoundTrip(t *testing.T, c *websocket.Conn, cmds ...*ultimav1.Command) []*ultimav1.CommandResponse {
	t.Helper()
	for _, cmd := range cmds {
		b, err := proto.Marshal(cmd)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.WriteMessage(websocket.BinaryMessage, b); err != nil {
			t.Fatal(err)
		}
	}
	out := make([]*ultimav1.CommandResponse, 0, len(cmds))
	for range cmds {
		out = append(out, wsRecv(t, c))
	}
	return out
}

func wsRecv(t *testing.T, c *websocket.Conn) *ultimav1.CommandResponse {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	mt, payload, err := c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if mt != websocket.BinaryMessage {
		t.Fatalf("WS reply frame type = %d, want binary", mt)
	}
	r := &ultimav1.CommandResponse{}
	if err := proto.Unmarshal(payload, r); err != nil {
		t.Fatal(err)
	}
	return r
}

func wsGeneric(name string, args ...string) *ultimav1.Command {
	g := &ultimav1.CommandRequest{Command: name}
	for _, s := range args {
		g.Args = append(g.Args, []byte(s))
	}
	return &ultimav1.Command{Cmd: &ultimav1.Command_Generic{Generic: g}}
}

func TestWSRoundTrip(t *testing.T) {
	_, url := wsTestServer(t)
	c := wsDial(t, url)

	replies := wsRoundTrip(t, c,
		&ultimav1.Command{Seq: 5, Cmd: &ultimav1.Command_Set{Set: &ultimav1.SetCommand{
			Key: []byte("w:k"), Value: []byte("v1"), TtlMs: 60000}}},
		&ultimav1.Command{Seq: 6, Cmd: &ultimav1.Command_Ttl{Ttl: &ultimav1.TtlCommand{
			Key: []byte("w:k")}}},
		&ultimav1.Command{Seq: 7, Cmd: &ultimav1.Command_Sadd{Sadd: &ultimav1.SAddCommand{
			Key: []byte("w:s"), Members: [][]byte{[]byte("a"), []byte("b")}}}},
		&ultimav1.Command{Seq: 8, Cmd: &ultimav1.Command_Sismember{Sismember: &ultimav1.SIsMemberCommand{
			Key: []byte("w:s"), Member: []byte("b")}}},
	)
	for i, r := range replies {
		if r.Seq != uint64(i+5) { //nolint:gosec // small test constants
			t.Errorf("reply %d seq = %d, want %d", i, r.Seq, i+5)
		}
	}
	if got := replies[0].Reply.GetSimpleString(); got != "OK" {
		t.Errorf("SET reply = %q, want OK", got)
	}
	if got := replies[1].Reply.GetInt(); got <= 0 || got > 60000 {
		t.Errorf("PTTL reply = %d, want (0, 60000]", got)
	}
	if got := replies[2].Reply.GetInt(); got != 2 {
		t.Errorf("SADD reply = %d, want 2", got)
	}
	if got := replies[3].Reply.GetInt(); got != 1 {
		t.Errorf("SISMEMBER reply = %d, want 1", got)
	}
}

// TestWSGenericAndErrors: the escape hatch and engine error strings as
// error values; text frames and garbage frames get error replies, not a
// dropped connection.
func TestWSGenericAndErrors(t *testing.T) {
	_, url := wsTestServer(t)
	c := wsDial(t, url)

	replies := wsRoundTrip(t, c,
		wsGeneric("set", "w:g", "hello"),
		wsGeneric("append", "w:g", "!"),
		wsGeneric("get", "w:g"),
	)
	if got := replies[2].Reply.GetBlobString(); string(got) != "hello!" {
		t.Errorf("generic round-trip reply = %q, want hello!", got)
	}

	// Text frame: error reply, connection stays up.
	if err := c.WriteMessage(websocket.TextMessage, []byte("PING")); err != nil {
		t.Fatal(err)
	}
	if got := wsRecv(t, c).Reply.GetError(); !strings.HasPrefix(got, "ERR /ws/v1 carries binary") {
		t.Errorf("text frame reply = %q, want binary-only error", got)
	}
	// Garbage binary frame: error reply, connection stays up.
	if err := c.WriteMessage(websocket.BinaryMessage, []byte{0xff, 0xff, 0xff}); err != nil {
		t.Fatal(err)
	}
	if got := wsRecv(t, c).Reply.GetError(); !strings.HasPrefix(got, "ERR frame is not a protobuf Command") {
		t.Errorf("garbage frame reply = %q, want unmarshal error", got)
	}
	// Still alive:
	replies = wsRoundTrip(t, c, wsGeneric("ping"))
	if got := replies[0].Reply.GetSimpleString(); got != "PONG" {
		t.Errorf("PING after bad frames = %q, want PONG", got)
	}
}

// TestWSPubSubPush subscribes over the generic envelope and verifies the
// broker's delivery arrives as an unsolicited seq-0 push frame (the WS
// twin of the RESP push path, design doc §6.3).
func TestWSPubSubPush(t *testing.T) {
	eng, url := wsTestServer(t)
	sub := wsDial(t, url)
	pub := wsDial(t, url)

	// SUBSCRIBE returns its ack as the command reply (a RESP3 push value).
	acks := wsRoundTrip(t, sub, wsGeneric("subscribe", "w:chan"))
	elems := acks[0].Reply.GetPush().GetElems()
	if len(elems) != 3 || string(elems[0].GetBlobString()) != "subscribe" {
		t.Fatalf("SUBSCRIBE ack = %v, want [subscribe w:chan 1]", acks[0].Reply)
	}

	if got := wsRoundTrip(t, pub, wsGeneric("publish", "w:chan", "hello-ws"))[0].Reply.GetInt(); got != 1 {
		t.Fatalf("PUBLISH reply = %d, want 1 subscriber", got)
	}

	push := wsRecv(t, sub)
	if push.Seq != 0 {
		t.Errorf("push seq = %d, want 0 (unsolicited)", push.Seq)
	}
	pe := push.Reply.GetPush().GetElems()
	if len(pe) != 3 ||
		string(pe[0].GetBlobString()) != "message" ||
		string(pe[1].GetBlobString()) != "w:chan" ||
		string(pe[2].GetBlobString()) != "hello-ws" {
		t.Errorf("push frame = %v, want [message w:chan hello-ws]", push.Reply)
	}
	_ = eng
}
