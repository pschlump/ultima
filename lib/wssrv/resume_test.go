package wssrv

// End-to-end tests for M6b resumable WS sessions (design doc §9.4, D18)
// over real websocket connections: the handshake, push_seq stamping,
// replay on reconnect with no gap and no duplicates, SESSION_EXPIRED on
// unknown/gapped/retention-expired resumes, takeover, and ABORTED frames
// for in-flight replies lost with a dropped connection.

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/wssession"
)

// newResumeServer boots /ws/v1 with a session registry of the given
// bounds and returns the engine, registry, and ws:// URL.
func newResumeServer(t *testing.T, window time.Duration, maxMsgs int) (*commands.Engine, *wssession.Registry, string) {
	t.Helper()
	shards := shard.NewEngine(4, 16)
	t.Cleanup(shards.Close)
	eng := commands.NewEngine(shards, "test", 0)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := wssession.NewRegistry(eng, window, maxMsgs, logger)
	t.Cleanup(reg.Close)
	mux := http.NewServeMux()
	mux.HandleFunc("/ws/v1", Handler(eng, nil, reg, logger, nil))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return eng, reg, "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/v1"
}

// handshakeNew requests a fresh session and returns its id.
func handshakeNew(t *testing.T, c *websocket.Conn) string {
	t.Helper()
	sendCmd(t, c, &ultimav1.Command{Seq: 1})
	r := recv(t, c)
	if r.GetSession() == "" || r.GetReply().GetSimpleString() != "OK" {
		t.Fatalf("new-session handshake reply = %v", r)
	}
	return r.GetSession()
}

// resume presents session + lastSeq and returns the first reply frame
// (OK or SESSION_EXPIRED).
func resume(t *testing.T, c *websocket.Conn, session string, lastSeq uint64) *ultimav1.CommandResponse {
	t.Helper()
	sendCmd(t, c, &ultimav1.Command{Seq: 1, Session: session, LastPushSeq: lastSeq})
	return recv(t, c)
}

// recvPush reads one frame and asserts it is a push with the given
// push_seq and payload.
func recvPush(t *testing.T, c *websocket.Conn, wantSeq uint64, wantPayload string) {
	t.Helper()
	r := recv(t, c)
	if r.GetSeq() != 0 || r.GetPushSeq() != wantSeq {
		t.Fatalf("push frame seq=%d push_seq=%d, want seq 0 push_seq %d (%v)",
			r.GetSeq(), r.GetPushSeq(), wantSeq, r)
	}
	elems := r.GetReply().GetPush().GetElems()
	if len(elems) != 3 || string(elems[2].GetBlobString()) != wantPayload {
		t.Fatalf("push payload = %v, want %q", r.GetReply(), wantPayload)
	}
}

func TestSessionHandshakeNew(t *testing.T) {
	_, _, url := newResumeServer(t, 0, 0)
	c := dial(t, url)
	sess := handshakeNew(t, c)

	// Commands work on the sessioned connection, and a second handshake
	// is rejected.
	sendCmd(t, c, generic(2, "ping"))
	if r := recv(t, c); r.GetSeq() != 2 || r.GetReply().GetSimpleString() != "PONG" {
		t.Fatalf("PING reply = %v", r)
	}
	sendCmd(t, c, &ultimav1.Command{Seq: 3})
	if r := recv(t, c); !strings.Contains(r.GetReply().GetError(), "session already negotiated") {
		t.Fatalf("second handshake reply = %v, want already-negotiated error", r)
	}
	_ = sess
}

func TestSessionResumeReplay(t *testing.T) {
	eng, _, url := newResumeServer(t, 0, 0)

	c1 := dial(t, url)
	sess := handshakeNew(t, c1)
	sendCmd(t, c1, generic(2, "subscribe", "rs-ch"))
	if r := recv(t, c1); r.GetReply().GetPush() == nil {
		t.Fatalf("subscribe ack = %v", r)
	}
	eng.PubSub.Publish("rs-ch", []byte("a"), 0, nil)
	eng.PubSub.Publish("rs-ch", []byte("b"), 0, nil)
	recvPush(t, c1, 1, "a")
	recvPush(t, c1, 2, "b")
	_ = c1.Close() // abrupt drop

	// Detached pushes buffer (whether or not the server has noticed the
	// drop yet — the resume's takeover path covers the race).
	eng.PubSub.Publish("rs-ch", []byte("c"), 0, nil)
	eng.PubSub.Publish("rs-ch", []byte("d"), 0, nil)

	c2 := dial(t, url)
	r := resume(t, c2, sess, 2)
	if r.GetSession() != sess || r.GetReply().GetSimpleString() != "OK" {
		t.Fatalf("resume reply = %v, want OK with session %q", r, sess)
	}
	recvPush(t, c2, 3, "c")
	recvPush(t, c2, 4, "d")

	// Live delivery resumes on the new connection, seqs continuing.
	eng.PubSub.Publish("rs-ch", []byte("e"), 0, nil)
	recvPush(t, c2, 5, "e")

	// The subscription survived: still exactly one subscriber.
	if n := eng.PubSub.NumSub("rs-ch"); n[0] != 1 {
		t.Fatalf("numsub = %d, want 1 (no duplicate registration)", n[0])
	}
}

func TestSessionResumeUnknown(t *testing.T) {
	_, _, url := newResumeServer(t, 0, 0)
	c := dial(t, url)
	r := resume(t, c, "no-such-session", 0)
	if !strings.HasPrefix(r.GetReply().GetError(), "SESSION_EXPIRED") {
		t.Fatalf("unknown-session resume = %v, want SESSION_EXPIRED", r)
	}
	if r.GetSession() != "no-such-session" {
		t.Fatalf("SESSION_EXPIRED frame session = %q, want echoed", r.GetSession())
	}
	// The connection is still usable, sessionless.
	sendCmd(t, c, generic(9, "ping"))
	if r := recv(t, c); r.GetReply().GetSimpleString() != "PONG" {
		t.Fatalf("PING after SESSION_EXPIRED = %v", r)
	}
}

func TestSessionGapExpires(t *testing.T) {
	eng, _, url := newResumeServer(t, 0, 2) // tiny buffer: 2 messages

	c1 := dial(t, url)
	sess := handshakeNew(t, c1)
	sendCmd(t, c1, generic(2, "subscribe", "gap-ch"))
	_ = recv(t, c1)
	_ = c1.Close()

	for _, m := range []string{"1", "2", "3", "4", "5"} {
		eng.PubSub.Publish("gap-ch", []byte(m), 0, nil)
	}

	// last_push_seq 0 needs pushes 1.., but the buffer holds only 4,5.
	c2 := dial(t, url)
	if r := resume(t, c2, sess, 0); !strings.HasPrefix(r.GetReply().GetError(), "SESSION_EXPIRED") {
		t.Fatalf("gapped resume = %v, want SESSION_EXPIRED", r)
	}
	// A resume inside the retained tail still works on the same conn.
	r := resume(t, c2, sess, 4)
	if r.GetReply().GetSimpleString() != "OK" {
		t.Fatalf("in-window resume = %v, want OK", r)
	}
	recvPush(t, c2, 5, "5")
}

func TestSessionRetentionExpiry(t *testing.T) {
	eng, _, url := newResumeServer(t, 100*time.Millisecond, 0)

	c1 := dial(t, url)
	sess := handshakeNew(t, c1)
	sendCmd(t, c1, generic(2, "subscribe", "exp-ch"))
	_ = recv(t, c1)
	_ = c1.Close()
	if n := eng.PubSub.NumSub("exp-ch"); n[0] != 1 {
		t.Fatalf("numsub right after drop = %d, want 1 (retained)", n[0])
	}

	time.Sleep(300 * time.Millisecond) // past the retention window
	if n := eng.PubSub.NumSub("exp-ch"); n[0] != 0 {
		t.Fatalf("numsub after window = %d, want 0 (subscription released)", n[0])
	}
	c2 := dial(t, url)
	if r := resume(t, c2, sess, 0); !strings.HasPrefix(r.GetReply().GetError(), "SESSION_EXPIRED") {
		t.Fatalf("resume after retention window = %v, want SESSION_EXPIRED", r)
	}
}

func TestSessionTakeover(t *testing.T) {
	eng, _, url := newResumeServer(t, 0, 0)

	c1 := dial(t, url)
	sess := handshakeNew(t, c1)
	sendCmd(t, c1, generic(2, "subscribe", "to-ch"))
	_ = recv(t, c1)
	eng.PubSub.Publish("to-ch", []byte("x"), 0, nil)
	recvPush(t, c1, 1, "x")

	// A second connection resumes the still-attached session: c1 is
	// closed by the takeover.
	c2 := dial(t, url)
	if r := resume(t, c2, sess, 1); r.GetReply().GetSimpleString() != "OK" {
		t.Fatalf("takeover resume = %v, want OK", r)
	}
	_ = c1.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, _, err := c1.ReadMessage(); err == nil {
		t.Fatal("displaced connection still open after takeover")
	}
	eng.PubSub.Publish("to-ch", []byte("y"), 0, nil)
	recvPush(t, c2, 2, "y")
}

func TestSessionAbortedReply(t *testing.T) {
	eng, _, url := newResumeServer(t, 0, 0)

	c1 := dial(t, url)
	sess := handshakeNew(t, c1)

	// A reply too big for the kernel buffers: the writer blocks mid-write
	// and the client's close turns the loss into an ABORTED record.
	big := []byte(strings.Repeat("x", 8<<20))
	sendCmd(t, c1, &ultimav1.Command{Seq: 2, Cmd: &ultimav1.Command_Set{Set: &ultimav1.SetCommand{
		Key: []byte("rs:big"), Value: big}}})
	if r := recv(t, c1); r.GetReply().GetSimpleString() != "OK" {
		t.Fatalf("SET reply = %v", r)
	}
	sendCmd(t, c1, generic(7, "get", "rs:big"))
	_ = c1.Close() // never read the 8MB reply
	if n := eng.PubSub.NumChannels(); n != 0 {
		t.Fatalf("unexpected channels: %d", n)
	}
	time.Sleep(300 * time.Millisecond) // let the server notice the drop

	c2 := dial(t, url)
	if r := resume(t, c2, sess, 0); r.GetReply().GetSimpleString() != "OK" {
		t.Fatalf("resume = %v, want OK", r)
	}
	r := recv(t, c2)
	if r.GetSeq() != 7 || !strings.HasPrefix(r.GetReply().GetError(), "ABORTED") {
		t.Fatalf("post-resume frame = %v, want ABORTED for seq 7", r)
	}
}

func TestSessionSlowConsumerNoGapNoDup(t *testing.T) {
	eng, _, url := newResumeServer(t, 30*time.Second, 20000)

	c := dial(t, url)
	sess := handshakeNew(t, c)
	sendCmd(t, c, generic(2, "subscribe", "sc-ch"))
	_ = recv(t, c)
	// From here the client reads only in recovery bursts: the outbound
	// queue overflows mid-flood and the connection dies, repeatedly.

	const total = 9000
	go func() {
		payload := []byte(strings.Repeat("x", 64))
		for i := 0; i < total; i++ {
			eng.PubSub.Publish("sc-ch", payload, 0, nil)
			if i%500 == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	}()

	seen := map[uint64]bool{}
	var last uint64
	deadline := time.Now().Add(30 * time.Second)
	for int(last) < total {
		if time.Now().After(deadline) {
			t.Fatalf("timed out: recovered %d/%d pushes", last, total)
		}
		if last > 0 { // reconnect and resume
			c = dial(t, url)
			r := resume(t, c, sess, last)
			if r.GetReply().GetSimpleString() != "OK" {
				t.Fatalf("resume at %d = %v, want OK", last, r)
			}
		}
		// Read until the connection dies under us (or we finish).
		_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
		for int(last) < total {
			mt, payload, err := c.ReadMessage()
			if err != nil {
				break // dropped again; outer loop resumes
			}
			if mt != websocket.BinaryMessage {
				t.Fatalf("frame type = %d", mt)
			}
			r := &ultimav1.CommandResponse{}
			if err := proto.Unmarshal(payload, r); err != nil {
				t.Fatal(err)
			}
			if r.GetPushSeq() == 0 {
				continue // subscribe ack or similar
			}
			ps := r.GetPushSeq()
			if seen[ps] {
				t.Fatalf("duplicate push_seq %d", ps)
			}
			if ps != last+1 {
				t.Fatalf("gap: got push_seq %d after %d", ps, last)
			}
			seen[ps] = true
			last = ps
		}
		_ = c.Close()
	}
}
