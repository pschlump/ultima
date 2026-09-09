package respserver

// Package-local tests for the RESP front-end push-queue contract: once a
// connection subscribes, every write (subscribe acks, command replies,
// async broker pushes) flows through the single bounded push queue in
// issue order, and a full queue — a slow consumer — closes the connection
// (the Redis client-output-buffer-limit analogue for pub/sub clients).

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/shard"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// newTestServer boots the RESP surface on an ephemeral port, wired exactly
// like cmd/ultima-server (New over a small shard engine).
func newTestServer(t *testing.T) (addr string, eng *commands.Engine) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	shards := shard.NewEngine(4, 16)
	t.Cleanup(shards.Close)
	eng = commands.NewEngine(shards, "test", 0)
	srv := New("127.0.0.1:0", eng)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { _ = srv.Close() })
	return lis.Addr().String(), eng
}

// testClient is a minimal raw-socket RESP client decoding one frame at a
// time into a line-oriented string form for assertions (same idiom as the
// m3 harness in tests/, which cannot be imported).
type testClient struct {
	conn net.Conn
	r    *bufio.Reader
}

func dial(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c := &testClient{conn: conn, r: bufio.NewReader(conn)}
	t.Cleanup(func() { _ = conn.Close() })
	return c
}

func (c *testClient) send(t *testing.T, args ...string) {
	t.Helper()
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := io.WriteString(c.conn, sb.String()); err != nil {
		t.Fatal(err)
	}
}

// recv reads one RESP frame (any type) and renders it canonically:
// aggregates as "(kind elem elem ...)" with nested frames recursed,
// null bulk as "nil".
func (c *testClient) recv(t *testing.T) string {
	t.Helper()
	if err := c.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	v, err := c.readVal()
	_ = c.conn.SetReadDeadline(time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func (c *testClient) readVal() (string, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "", err
	}
	body := strings.TrimRight(line[1:], "\r\n")
	switch line[0] {
	case '+', '-', ':', ',', '#', '(':
		return string(line[0]) + body, nil
	case '_':
		return "nil", nil
	case '$', '=':
		n, err := strconv.Atoi(body)
		if err != nil {
			return "", err
		}
		if n < 0 {
			return "nil", nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			return "", err
		}
		return string(buf[:n]), nil
	case '*', '%', '~', '>':
		n, err := strconv.Atoi(body)
		if err != nil {
			return "", err
		}
		if n < 0 {
			return "nil", nil
		}
		if line[0] == '%' {
			n *= 2
		}
		parts := make([]string, n)
		for i := range parts {
			p, err := c.readVal()
			if err != nil {
				return "", err
			}
			parts[i] = p
		}
		return string(line[0]) + "(" + strings.Join(parts, " ") + ")", nil
	}
	return "", fmt.Errorf("unknown reply type %q", line)
}

// do sends a command and reads its reply.
func (c *testClient) do(t *testing.T, args ...string) string {
	t.Helper()
	c.send(t, args...)
	return c.recv(t)
}

// TestPushModeHappyPathOrdering: after the first subscription, command
// replies and async pushes share the one queue and reach the client in
// exact issue order.
func TestPushModeHappyPathOrdering(t *testing.T) {
	addr, _ := newTestServer(t)
	sub, pub := dial(t, addr), dial(t, addr)

	if got := sub.do(t, "HELLO", "3"); !strings.HasPrefix(got, "%(") {
		t.Fatalf("HELLO 3 = %q", got)
	}
	if got := sub.do(t, "SUBSCRIBE", "ord"); got != ">(subscribe ord :1)" {
		t.Fatalf("subscribe ack = %q", got)
	}

	// A publish from a second connection arrives as a push frame.
	if got := pub.do(t, "PUBLISH", "ord", "m1"); got != ":1" {
		t.Fatalf("publish = %q", got)
	}
	if got := sub.recv(t); got != ">(message ord m1)" {
		t.Fatalf("delivery = %q", got)
	}

	// Command replies still arrive after push mode started (RESP3 is not
	// subscribe-gated), interleaved with pushes, one op at a time so the
	// order assertion is deterministic.
	if got := sub.do(t, "PING"); got != "+PONG" {
		t.Fatalf("PING in push mode = %q", got)
	}
	if got := pub.do(t, "PUBLISH", "ord", "m2"); got != ":1" {
		t.Fatalf("publish = %q", got)
	}
	if got := sub.recv(t); got != ">(message ord m2)" {
		t.Fatalf("delivery = %q", got)
	}
	if got := sub.do(t, "PING"); got != "+PONG" {
		t.Fatalf("second PING = %q", got)
	}

	// Pipelined on the subscribed connection: PING then a self-addressed
	// PUBLISH. The single queue must emit +PONG, the :1 publish reply, and
	// only then the postponed self push (Redis pending_push_messages
	// semantics), in that exact wire order.
	sub.send(t, "PING")
	sub.send(t, "PUBLISH", "ord", "self")
	if got := sub.recv(t); got != "+PONG" {
		t.Fatalf("pipelined PING reply = %q", got)
	}
	if got := sub.recv(t); got != ":1" {
		t.Fatalf("self publish reply = %q", got)
	}
	if got := sub.recv(t); got != ">(message ord self)" {
		t.Fatalf("self publish push = %q", got)
	}
}

// TestPushQueueBoundaryHolds: exactly pushQueueCap undrained messages must
// NOT close the connection — enqueue only drops when the queue is already
// full, and the buffered channel holds exactly cap items. The client stops
// reading, the queue (plus kernel buffers) absorbs the burst, and every
// frame is delivered when the client starts reading again.
func TestPushQueueBoundaryHolds(t *testing.T) {
	addr, eng := newTestServer(t)
	sub := dial(t, addr)

	if got := sub.do(t, "SUBSCRIBE", "bounded"); got != "*(subscribe bounded :1)" {
		t.Fatalf("subscribe ack = %q", got)
	}

	// Never read during the burst. Publish exactly cap messages; none may
	// be dropped and the connection must survive.
	payload := strings.Repeat("x", 32)
	for i := 0; i < pushQueueCap; i++ {
		if n := eng.PubSub.Publish("bounded", []byte(payload), 0, nil); n != 1 {
			t.Fatalf("publish %d delivered to %d subscribers, want 1 (connection dropped at/below cap)", i, n)
		}
	}

	// Drain: exactly pushQueueCap push frames, all intact and in order.
	want := "*(message bounded " + payload + ")"
	for i := 0; i < pushQueueCap; i++ {
		if got := sub.recv(t); got != want {
			t.Fatalf("frame %d = %q, want %q", i, got, want)
		}
	}
	// Still alive: RESP2 subscribe-mode PING answers through the queue.
	if got := sub.do(t, "PING"); got != "*(pong )" {
		t.Fatalf("PING after cap-size burst = %q", got)
	}
}

// TestPushQueueSlowConsumer: a subscriber that never reads its socket must
// be disconnected once the outbound queue overflows. Publishing far beyond
// pushQueueCap (the writer stalls on a full socket buffer, the queue fills,
// the next enqueue closes the connection) must trip the limit; the blocked
// reader observes the queued frames followed by end-of-stream, not a hang.
func TestPushQueueSlowConsumer(t *testing.T) {
	addr, eng := newTestServer(t)
	sub := dial(t, addr)

	if got := sub.do(t, "SUBSCRIBE", "slow"); got != "*(subscribe slow :1)" {
		t.Fatalf("subscribe ack = %q", got)
	}
	// From here on the client reads nothing.

	// Flood well past the queue cap. Publish's delivery count drops to 0
	// once the closed callback has deregistered the subscription, which is
	// the observable signal that the connection was torn down.
	payload := []byte(strings.Repeat("x", 64))
	const maxPublishes = 64 * pushQueueCap
	closed := false
	published := 0
	for ; published < maxPublishes; published++ {
		if eng.PubSub.Publish("slow", payload, 0, nil) == 0 {
			closed = true
			break
		}
	}
	if !closed {
		// The final enqueue may have closed the connection with cleanup
		// still in flight; wait briefly for the closed callback.
		deadline := time.Now().Add(5 * time.Second)
		for eng.PubSub.NumChannels() != 0 {
			if time.Now().After(deadline) {
				t.Fatalf("subscription still registered after %d publishes; slow consumer was not dropped", published)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	t.Logf("connection closed after %d publishes (queue cap %d)", published, pushQueueCap)

	// The blocked reader drains what the queue and kernel buffers held,
	// then sees the stream terminate (FIN after a graceful close), within
	// a bounded time — never a silent hang.
	if err := sub.conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 32<<10)
	drained := 0
	for {
		n, err := sub.r.Read(buf) // bufio may already hold read-ahead frames
		drained += n
		if err == nil {
			continue
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			t.Fatal("read timed out: server never closed the slow consumer's connection")
		}
		t.Logf("blocked reader observed terminal error after %d bytes: %v", drained, err)
		break
	}
	if drained == 0 {
		t.Fatal("no queued frames were delivered before the close")
	}
}
