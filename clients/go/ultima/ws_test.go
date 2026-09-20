package ultima

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pschlump/ultima/lib/resp"
)

// pushRecorder collects OnPush deliveries.
type pushRecorder struct {
	mu    sync.Mutex
	items []pushItem
}

type pushItem struct {
	v       resp.Value
	pushSeq uint64
}

func (p *pushRecorder) handler() func(resp.Value, uint64) {
	return func(v resp.Value, seq uint64) {
		p.mu.Lock()
		p.items = append(p.items, pushItem{v: v, pushSeq: seq})
		p.mu.Unlock()
	}
}

// waitPayload blocks until a push with [message channel payload] arrives
// and returns its push_seq.
func (p *pushRecorder) waitPayload(t *testing.T, channel, payload string) uint64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		for _, it := range p.items {
			parts, ok := AsStringSlice(it.v)
			if ok && len(parts) == 3 && parts[0] == "message" && parts[1] == channel && parts[2] == payload {
				seq := it.pushSeq
				p.mu.Unlock()
				return seq
			}
		}
		p.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	t.Fatalf("no push [%s %s]; got %v", channel, payload, p.items)
	return 0
}

// TestWSRoundTrip covers the typed helpers over /ws/v1 (§6.3).
func TestWSRoundTrip(t *testing.T) {
	env := newTestEnv(t, false, 0)
	c, err := DialWS(WSOptions{Addr: env.httpAddr})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	if v, err := c.Ping(); err != nil || v.Str != "PONG" {
		t.Fatalf("Ping = %v, %v", v, err)
	}
	if v, err := c.Set("m7:w:s", "42"); err != nil || v.Str != "OK" {
		t.Fatalf("Set = %v, %v", v, err)
	}
	if s, _ := AsString(asVal(t)(c.Get("m7:w:s"))); s != "42" {
		t.Fatalf("Get = %q", s)
	}
	if n, _ := AsInt(asVal(t)(c.IncrBy("m7:w:s", 8))); n != 50 {
		t.Fatalf("IncrBy = %d", n)
	}
	if v, err := c.Exec("TYPE", "m7:w:s"); err != nil || v.Str != "string" {
		t.Fatalf("generic TYPE = %v, %v", v, err)
	}
	// Error replies arrive as KindError values.
	if v, err := c.Get("m7:w:nope"); err != nil || !IsNull(v) {
		t.Fatalf("Get missing = %v, %v", v, err)
	}
	if v, _ := c.Exec("BOGUS"); v.Kind != resp.KindError {
		t.Fatalf("BOGUS = %v, want error reply", v)
	}
	if c.SessionID() == "" {
		t.Fatal("no §9.4 session id after connect")
	}
}

// TestWSSubscribePush: deliveries arrive via OnPush, stamped with
// push_seq (sessioned connection, §9.4).
func TestWSSubscribePush(t *testing.T) {
	env := newTestEnv(t, false, 0)
	rec := &pushRecorder{}
	c, err := DialWS(WSOptions{Addr: env.httpAddr, OnPush: rec.handler()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	if _, err := c.Subscribe("m7:w:ch"); err != nil {
		t.Fatal(err)
	}
	pub, err := DialRESP(env.respAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pub.Close() }()
	if n, _ := AsInt(mustVal(t, pub, "PUBLISH", "m7:w:ch", "m1")); n != 1 {
		t.Fatalf("PUBLISH receivers = %d, want 1", n)
	}
	if seq := rec.waitPayload(t, "m7:w:ch", "m1"); seq == 0 {
		t.Fatal("push_seq = 0 on a sessioned connection")
	}
}

// killConn abruptly drops the underlying websocket, simulating a network
// failure (no close frame) — the server detaches the §9.4 session and the
// client's supervisor reconnects and resumes.
func killConn(t *testing.T, c *WSClient) {
	t.Helper()
	conn := c.currentConn()
	if conn == nil {
		t.Fatal("no connection to kill")
	}
	_ = conn.Close()
	deadline := time.Now().Add(3 * time.Second)
	for c.Open() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if c.Open() {
		t.Fatal("client still reports open after kill")
	}
}

// TestWSSessionResumeReplay (§9.4, D18): kill the connection, publish
// while detached, reconnect — the missed push is replayed from the
// session buffer and the subscription is retained (no re-subscribe).
func TestWSSessionResumeReplay(t *testing.T) {
	env := newTestEnv(t, false, 0)
	rec := &pushRecorder{}
	resumed := make(chan struct{}, 1)
	c, err := DialWS(WSOptions{
		Addr:         env.httpAddr,
		OnPush:       rec.handler(),
		OnResume:     func() { resumed <- struct{}{} },
		ReconnectMin: 400 * time.Millisecond, // leave a window to publish mid-drop
		ReconnectMax: 400 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	if _, err := c.Subscribe("m7:w:rx"); err != nil {
		t.Fatal(err)
	}
	pub, err := DialRESP(env.respAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pub.Close() }()

	if _, err := pub.Exec("PUBLISH", "m7:w:rx", "before"); err != nil {
		t.Fatal(err)
	}
	seq1 := rec.waitPayload(t, "m7:w:rx", "before")

	// Drop the connection; publish while the client is down; the §9.4
	// buffer must replay it after the resume handshake.
	killConn(t, c)
	time.Sleep(150 * time.Millisecond) // server-side detach settles
	if n, _ := AsInt(mustVal(t, pub, "PUBLISH", "m7:w:rx", "during")); n != 1 {
		t.Fatalf("PUBLISH during drop = %d receivers, want 1 (subscription retained)", n)
	}

	seq2 := rec.waitPayload(t, "m7:w:rx", "during")
	if seq2 <= seq1 {
		t.Fatalf("replayed push_seq %d not after %d", seq2, seq1)
	}
	select {
	case <-resumed:
	default:
		t.Fatal("OnResume did not fire")
	}

	// The session is live again end to end.
	if _, err := pub.Exec("PUBLISH", "m7:w:rx", "after"); err != nil {
		t.Fatal(err)
	}
	rec.waitPayload(t, "m7:w:rx", "after")
}

// TestWSSessionExpiredGap: a reconnect after the retention window gets
// SESSION_EXPIRED — the client fires OnGap, starts a fresh session and
// transparently re-subscribes (§9.4's explicit-never-silent fallback).
func TestWSSessionExpiredGap(t *testing.T) {
	env := newTestEnv(t, false, 250*time.Millisecond) // short retention window
	rec := &pushRecorder{}
	gap := make(chan struct{}, 1)
	c, err := DialWS(WSOptions{
		Addr:   env.httpAddr,
		OnPush: rec.handler(),
		OnGap:  func() { gap <- struct{}{} },
		// Reconnect must come AFTER the 250ms retention window lapses,
		// else the resume would succeed and there would be no gap.
		ReconnectMin: 500 * time.Millisecond,
		ReconnectMax: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	if _, err := c.Subscribe("m7:w:gap"); err != nil {
		t.Fatal(err)
	}
	killConn(t, c)
	time.Sleep(600 * time.Millisecond) // outlive the retention window

	select {
	case <-gap:
	case <-time.After(5 * time.Second):
		t.Fatal("OnGap did not fire after retention expiry")
	}

	// Re-subscribed on the fresh session: publishes flow again.
	pub, err := DialRESP(env.respAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pub.Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		// PUBSUB NUMSUB answers [channel, count]; the re-subscription is
		// visible when the count reaches 1.
		v := mustVal(t, pub, "PUBSUB", "NUMSUB", "m7:w:gap")
		if len(v.Arr) == 2 && v.Arr[1].Int >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("client did not re-subscribe after SESSION_EXPIRED; NUMSUB = %v", v)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := pub.Exec("PUBLISH", "m7:w:gap", "post-gap"); err != nil {
		t.Fatal(err)
	}
	rec.waitPayload(t, "m7:w:gap", "post-gap")
}

// TestWSSessionless: DisableSessions behaves like M4 — no handshake, and
// in-flight commands fail on drop.
func TestWSSessionless(t *testing.T) {
	env := newTestEnv(t, false, 0)
	c, err := DialWS(WSOptions{Addr: env.httpAddr, DisableSessions: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if c.SessionID() != "" {
		t.Fatalf("sessionless client has session %q", c.SessionID())
	}
	if v, err := c.Set("m7:w:sl", "x"); err != nil || v.Str != "OK" {
		t.Fatalf("Set = %v, %v", v, err)
	}
}

// TestWSMonitor: MONITOR via the generic envelope delivers command events
// as push frames (the M6d engine MONITOR over §6.3).
func TestWSMonitor(t *testing.T) {
	env := newTestEnv(t, false, 0)
	rec := &pushRecorder{}
	c, err := DialWS(WSOptions{Addr: env.httpAddr, OnPush: rec.handler()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if v, err := c.Exec("MONITOR"); err != nil || v.Str != "OK" {
		t.Fatalf("MONITOR = %v, %v", v, err)
	}

	pub, err := DialRESP(env.respAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pub.Close() }()
	if _, err := pub.Exec("SET", "m7:w:mon", "seen"); err != nil {
		t.Fatal(err)
	}

	// The monitor push is a single bulk-string line ("+… [db addr] …"
	// rendered by the engine), not a [message …] triple.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rec.mu.Lock()
		found := false
		for _, it := range rec.items {
			if s, ok := AsString(it.v); ok && strings.Contains(s, "SET") && strings.Contains(s, "m7:w:mon") {
				found = true
			} else if it.v.Kind == resp.KindPush {
				for _, e := range it.v.Arr {
					if s, ok := AsString(e); ok && strings.Contains(s, "m7:w:mon") {
						found = true
					}
				}
			}
		}
		rec.mu.Unlock()
		if found {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("MONITOR push for SET m7:w:mon never arrived")
}
