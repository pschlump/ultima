package ultima

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/resp"
)

// asStr/asInt/expectOK are t-bound assertion adapters shaped so the typed
// helpers' (Value, error) results can pipe straight in: asInt(c.Incr(k)).
func asStr(t *testing.T) func(resp.Value, error) string {
	return func(v resp.Value, err error) string {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		s, _ := AsString(v)
		return s
	}
}

func asInt(t *testing.T) func(resp.Value, error) int64 {
	return func(v resp.Value, err error) int64 {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		n, _ := AsInt(v)
		return n
	}
}

func asVal(t *testing.T) func(resp.Value, error) resp.Value {
	return func(v resp.Value, err error) resp.Value {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

func ok(t *testing.T) func(resp.Value, error) {
	return func(v resp.Value, err error) {
		t.Helper()
		if err != nil || v.Str != "OK" {
			t.Fatalf("reply = %v, %v; want OK", v, err)
		}
	}
}

func eqInt(t *testing.T, want int64) func(resp.Value, error) {
	return func(v resp.Value, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		if n, _ := AsInt(v); n != want {
			t.Fatalf("reply = %v, want (integer) %d", v, want)
		}
	}
}

// TestGRPCTypedHelpers walks the typed-command surface (§6.2): one helper
// per family, asserting the engine's replies come through the bidi stream
// with full fidelity.
func TestGRPCTypedHelpers(t *testing.T) {
	env := newTestEnv(t, false, 0)
	c, err := DialGRPC(env.grpcAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx := context.Background()

	if msg, err := c.Ping(ctx, ""); err != nil || msg != "PONG" {
		t.Fatalf("Ping = %q, %v", msg, err)
	}

	str, num := asStr(t), asInt(t)

	ok(t)(c.Set("m7:g:s", "1"))
	if s := str(c.Get("m7:g:s")); s != "1" {
		t.Fatalf("Get = %q", s)
	}
	eqInt(t, 2)(c.Incr("m7:g:s"))
	eqInt(t, 7)(c.IncrBy("m7:g:s", 5))
	eqInt(t, 6)(c.Decr("m7:g:s"))
	eqInt(t, 4)(c.DecrBy("m7:g:s", 2))
	// INCRBYFLOAT answers a bulk string (string2d-exact), like Redis.
	if s := str(c.IncrByFloat("m7:g:f", 1.5)); s != "1.5" {
		t.Fatalf("IncrByFloat = %q", s)
	}
	ok(t)(c.MSet("m7:g:a", "1", "m7:g:b", "2"))
	if vals, _ := AsStringSlice(asVal(t)(c.MGet("m7:g:a", "m7:g:b"))); len(vals) != 2 || vals[0] != "1" || vals[1] != "2" {
		t.Fatalf("MGet = %v", vals)
	}
	eqInt(t, 2)(c.Append("m7:g:a", "x"))
	eqInt(t, 2)(c.Exists("m7:g:a", "m7:g:b", "m7:g:none"))
	eqInt(t, 1)(c.Expire("m7:g:a", 60*time.Second))
	if n := num(c.PTTL("m7:g:a")); n <= 0 || n > 60000 {
		t.Fatalf("PTTL = %d, want (0,60000]", n)
	}
	eqInt(t, 1)(c.Persist("m7:g:a"))
	eqInt(t, 2)(c.Del("m7:g:a", "m7:g:b"))

	// SetOptions: NX on an existing key is refused (null), GET returns the old value.
	if v, err := c.SetOpts("m7:g:s", "x", SetOptions{NX: true}); err != nil || !IsNull(v) {
		t.Fatalf("SET NX existing = %v, %v; want null", v, err)
	}
	if s := str(c.SetOpts("m7:g:s", "9", SetOptions{Get: true})); s != "4" {
		t.Fatalf("SET GET old value = %q, want 4", s)
	}

	// Hashes.
	eqInt(t, 2)(c.HSet("m7:g:h", "f1", "1", "f2", "2"))
	if s := str(c.HGet("m7:g:h", "f1")); s != "1" {
		t.Fatalf("HGet = %q", s)
	}
	eqInt(t, 11)(c.HIncrBy("m7:g:h", "f1", 10))
	eqInt(t, 1)(c.HDel("m7:g:h", "f2"))
	if m, _ := AsStringMap(asVal(t)(c.HGetAll("m7:g:h"))); m["f1"] != "11" {
		t.Fatalf("HGetAll = %v", m)
	}

	// Lists.
	eqInt(t, 2)(c.RPush("m7:g:l", "a", "b"))
	eqInt(t, 3)(c.LPush("m7:g:l", "z"))
	if vals, _ := AsStringSlice(asVal(t)(c.LRange("m7:g:l", 0, -1))); len(vals) != 3 || vals[0] != "z" || vals[2] != "b" {
		t.Fatalf("LRange = %v", vals)
	}
	eqInt(t, 3)(c.LLen("m7:g:l"))
	if s := str(c.LPop("m7:g:l")); s != "z" {
		t.Fatalf("LPop = %q", s)
	}
	if s := str(c.RPop("m7:g:l")); s != "b" {
		t.Fatalf("RPop = %q", s)
	}

	// Sets.
	eqInt(t, 2)(c.SAdd("m7:g:set", "x", "y"))
	eqInt(t, 1)(c.SIsMember("m7:g:set", "x"))
	eqInt(t, 1)(c.SRem("m7:g:set", "y"))
	if vals, _ := AsStringSlice(asVal(t)(c.SMembers("m7:g:set"))); len(vals) != 1 || vals[0] != "x" {
		t.Fatalf("SMembers = %v", vals)
	}

	// Sorted sets — including double fidelity through the typed ZADD:
	// protobuf carries the raw float64, and on the binary surfaces
	// (ConnState.Proto = 3) ZSCORE answers a RESP3 double — the exact
	// float64 must round-trip.
	score := 0.1 + 0.2 // 0.30000000000000004 — the classic fidelity probe
	eqInt(t, 2)(c.ZAdd("m7:g:z", ZMember{Score: score, Member: "a"}, ZMember{Score: 1.5, Member: "b"}))
	if got, ok := AsFloat(asVal(t)(c.ZScore("m7:g:z", "a"))); !ok || got != score {
		t.Fatalf("ZScore did not round-trip the exact double %v", score)
	}
	if vals, _ := AsStringSlice(asVal(t)(c.ZRange("m7:g:z", 0, -1))); len(vals) != 2 || vals[0] != "a" {
		t.Fatalf("ZRange = %v", vals)
	}
	// ZRANGE WITHSCORES on RESP3 answers member/score tuple pairs (RESP2
	// flattens them), each score an exact double.
	if v := asVal(t)(c.ZRangeWithScores("m7:g:z", 0, -1)); len(v.Arr) != 2 ||
		len(v.Arr[0].Arr) != 2 || v.Arr[0].Arr[1].Dbl != score || v.Arr[1].Arr[1].Dbl != 1.5 {
		t.Fatalf("ZRangeWithScores = %v", v)
	}
	eqInt(t, 2)(c.ZCard("m7:g:z"))
	// ZADD options: INCR returns the new score (a RESP3 double here), XX
	// on a missing member adds nothing.
	if got, _ := AsFloat(asVal(t)(c.ZAddOpts("m7:g:z", ZAddOptions{INCR: true}, ZMember{Score: 1, Member: "b"}))); got != 2.5 {
		t.Fatalf("ZADD INCR = %v, want 2.5", got)
	}
	eqInt(t, 0)(c.ZAddOpts("m7:g:z", ZAddOptions{XX: true}, ZMember{Score: 9, Member: "nope"}))
	eqInt(t, 1)(c.ZRem("m7:g:z", "b"))
}

// TestGRPCConcurrentExec hammers the bidi stream from many goroutines to
// shake the seq/pending correlation.
func TestGRPCConcurrentExec(t *testing.T) {
	env := newTestEnv(t, false, 0)
	c, err := DialGRPC(env.grpcAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	const workers = 16
	const per = 50
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			key := "m7:g:conc:" + strconv.Itoa(w)
			for i := 0; i < per; i++ {
				v, err := c.Incr(key)
				if err != nil {
					errs <- err
					return
				}
				if n, _ := AsInt(v); n != int64(i+1) {
					errs <- strconv.ErrSyntax
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

// TestGRPCExecBatch exercises the unary batch RPC.
func TestGRPCExecBatch(t *testing.T) {
	env := newTestEnv(t, false, 0)
	c, err := DialGRPC(env.grpcAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	vals, err := c.ExecBatch(context.Background(),
		[]string{"SET", "m7:g:batch", "5"},
		[]string{"INCR", "m7:g:batch"},
		[]string{"GET", "m7:g:batch"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 3 {
		t.Fatalf("ExecBatch returned %d replies, want 3", len(vals))
	}
	if vals[0].Str != "OK" || vals[1].Int != 6 {
		t.Fatalf("batch replies = %v", vals)
	}
	if s, _ := AsString(vals[2]); s != "6" {
		t.Fatalf("batch GET = %q", s)
	}
}

// TestGRPCSubscribeAndMonitor exercises the two server streams: a
// subscribe ack, a published PushEvent, and a MONITOR CommandEvent.
func TestGRPCSubscribeAndMonitor(t *testing.T) {
	env := newTestEnv(t, false, 0)
	sub, err := DialGRPC(env.grpcAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type push struct {
		channel, payload string
		ack              bool
	}
	pushes := make(chan push, 8)
	go func() {
		_ = sub.Subscribe(ctx, []string{"m7:g:ch"}, nil, func(ev *ultimav1.PushEvent) {
			pushes <- push{channel: ev.GetChannel(), payload: string(ev.GetPayload()), ack: ev.GetIsAck()}
		})
	}()

	// The subscribe ack arrives first.
	select {
	case p := <-pushes:
		if !p.ack || p.channel != "m7:g:ch" {
			t.Fatalf("first event = %+v, want subscribe ack", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no subscribe ack on gRPC stream")
	}

	// MONITOR observes commands from other connections.
	mon, err := DialGRPC(env.grpcAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mon.Close() }()
	monEvents := make(chan string, 8)
	go func() {
		_ = mon.Monitor(ctx, func(ev *ultimav1.CommandEvent) {
			monEvents <- ev.GetCommand()
		})
	}()
	// Give the monitor stream a beat to register before generating traffic.
	time.Sleep(100 * time.Millisecond)

	pub, err := DialRESP(env.respAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pub.Close() }()
	if _, err := pub.Exec("PUBLISH", "m7:g:ch", "hello-grpc"); err != nil {
		t.Fatal(err)
	}

	select {
	case p := <-pushes:
		if p.ack || p.channel != "m7:g:ch" || p.payload != "hello-grpc" {
			t.Fatalf("push = %+v, want message on m7:g:ch", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no published message on gRPC stream")
	}

	// The PUBLISH shows up on MONITOR (other commands may precede it).
	deadline := time.After(3 * time.Second)
	for {
		select {
		case cmd := <-monEvents:
			if cmd == "publish" || cmd == "PUBLISH" {
				return
			}
		case <-deadline:
			t.Fatal("MONITOR saw no PUBLISH event")
		}
	}
}
