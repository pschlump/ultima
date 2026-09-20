package ultima

import (
	"strings"
	"testing"
	"time"

	"github.com/pschlump/ultima/lib/resp"
)

func TestRESPRoundTrip(t *testing.T) {
	env := newTestEnv(t, false, 0)
	c, err := DialRESP(env.respAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	mustExec := func(want string, args ...any) resp.Value {
		t.Helper()
		v, err := c.Exec(args...)
		if err != nil {
			t.Fatalf("Exec %v: %s", args, err)
		}
		if want != "" {
			got, _ := AsString(v)
			if got != want {
				t.Fatalf("Exec %v = %v (string %q), want %q", args, v, got, want)
			}
		}
		return v
	}
	mustExec("PONG", "PING")
	mustExec("OK", "SET", "m7:r:k", "v1")
	if v := mustExec("v1", "GET", "m7:r:k"); v.Kind != resp.KindBlobString {
		t.Fatalf("GET kind = %v, want blob", v.Kind)
	}
	if v, _ := c.Exec("INCR", "m7:r:n"); v.Int != 1 {
		t.Fatalf("INCR = %v, want 1", v)
	}
	// Error replies arrive as KindError values with the byte-exact text.
	v, err := c.Exec("GET", "m7:r:missing")
	if err != nil || v.Kind != resp.KindNull {
		t.Fatalf("GET missing = %v, %v; want null", v, err)
	}
	v, _ = c.Exec("INCR", "m7:r:k")
	if v.Kind != resp.KindError || !strings.HasPrefix(v.Str, "ERR value is not an integer") {
		t.Fatalf("INCR on string = %v, want not-an-integer error", v)
	}
	// RESP2 downgrade: HGETALL comes back as a flat array.
	mustExec("", "HSET", "m7:r:h", "f", "1")
	if v, _ := c.Exec("HGETALL", "m7:r:h"); v.Kind != resp.KindArray {
		t.Fatalf("HGETALL (RESP2) kind = %v, want array", v.Kind)
	}
}

func TestRESP3Negotiation(t *testing.T) {
	env := newTestEnv(t, false, 0)
	c, err := DialRESP(env.respAddr, WithRESPProto(3))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, err := c.Exec("HSET", "m7:r3:h", "f", "1"); err != nil {
		t.Fatal(err)
	}
	v, err := c.Exec("HGETALL", "m7:r3:h")
	if err != nil {
		t.Fatal(err)
	}
	if v.Kind != resp.KindMap {
		t.Fatalf("HGETALL (RESP3) kind = %v, want map", v.Kind)
	}
	m, ok := AsStringMap(v)
	if !ok || m["f"] != "1" {
		t.Fatalf("HGETALL = %v, want f=1", v)
	}
}

func TestRESPRequirePass(t *testing.T) {
	env := newTestEnv(t, false, 0)
	env.eng.SetRequirePass("pw-m7")
	defer env.eng.SetRequirePass("")

	// No password: every command is NOAUTH.
	c, err := DialRESP(env.respAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	v, err := c.Exec("PING")
	if err != nil {
		t.Fatal(err)
	}
	if v.Kind != resp.KindError || !strings.HasPrefix(v.Str, "NOAUTH") {
		t.Fatalf("PING without auth = %v, want NOAUTH", v)
	}

	// Wrong password: dial fails on the AUTH reply.
	if _, err := DialRESP(env.respAddr, WithRESPPassword("wrong")); err == nil {
		t.Fatal("dial with wrong password succeeded")
	}

	// Right password: commands flow; SELECT happens after AUTH.
	c2, err := DialRESP(env.respAddr, WithRESPPassword("pw-m7"), WithRESPDB(3))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close() }()
	if v, _ := c2.Exec("PING"); v.Str != "PONG" {
		t.Fatalf("authed PING = %v", v)
	}
	if _, err := c2.Exec("SET", "m7:r:db3", "x"); err != nil {
		t.Fatal(err)
	}
	if n, _ := AsInt(mustVal(t, c2, "DBSIZE")); n != 1 {
		t.Fatalf("DBSIZE on db 3 = %d, want 1 (SELECT honored)", n)
	}
}

func mustVal(t *testing.T, c *RESPClient, args ...any) resp.Value {
	t.Helper()
	v, err := c.Exec(args...)
	if err != nil {
		t.Fatalf("Exec %v: %s", args, err)
	}
	return v
}

// TestRESPSubscribeStream exercises Stream: subscribe acks and published
// messages arrive as push frames until Close.
func TestRESPSubscribeStream(t *testing.T) {
	env := newTestEnv(t, false, 0)
	sub, err := DialRESP(env.respAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	pub, err := DialRESP(env.respAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pub.Close() }()

	frames := make(chan resp.Value, 8)
	streamErr := make(chan error, 1)
	go func() {
		streamErr <- sub.Stream(func(v resp.Value) { frames <- v }, "SUBSCRIBE", "m7:r:ch")
	}()

	// First frame: the subscribe ack.
	select {
	case v := <-frames:
		if v.Kind != resp.KindPush && v.Kind != resp.KindArray {
			t.Fatalf("subscribe ack kind = %v", v.Kind)
		}
		if s, _ := AsString(v.Arr[0]); s != "subscribe" {
			t.Fatalf("ack[0] = %v, want subscribe", v.Arr[0])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no subscribe ack")
	}

	if n, _ := AsInt(mustVal(t, pub, "PUBLISH", "m7:r:ch", "hello")); n != 1 {
		t.Fatalf("PUBLISH = %d receivers, want 1", n)
	}
	select {
	case v := <-frames:
		parts, ok := AsStringSlice(v)
		if !ok || len(parts) != 3 || parts[0] != "message" || parts[1] != "m7:r:ch" || parts[2] != "hello" {
			t.Fatalf("message frame = %v", v)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no message push")
	}

	// Close ends the stream with an error.
	_ = sub.Close()
	select {
	case err := <-streamErr:
		if err == nil {
			t.Fatal("Stream returned nil after Close")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Stream did not return after Close")
	}
}
