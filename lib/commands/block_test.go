package commands

import (
	"strings"
	"testing"
	"time"

	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
)

// Blocking command unit tests (M3 part 5): fast paths, timeout nulls,
// wake from a second connection, key priority, WRONGTYPE ordering, parse
// errors (all probed against Redis 7.2.7), blocking-inside-EXEC, live
// blocked_clients, and shutdown with a parked waiter.

// runBlocked runs a command in a goroutine and returns its result
// channel (buffered, so the goroutine never leaks past the test).
func runBlocked(e *Engine, cs *ConnState, args ...string) chan resp.Value {
	ch := make(chan resp.Value, 1)
	go func() {
		bb := make([][]byte, len(args))
		for i, a := range args {
			bb[i] = []byte(a)
		}
		ch <- e.Execute(cs, bb)
	}()
	return ch
}

// awaitBlocked waits until the engine reports n parked blocking clients.
func awaitBlocked(t *testing.T, e *Engine, n int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for e.blockedClients.Load() != n {
		if time.Now().After(deadline) {
			t.Fatalf("blocked_clients = %d, want %d", e.blockedClients.Load(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

func recvReply(t *testing.T, ch chan resp.Value) resp.Value {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("blocked command did not resolve within 2s")
		return resp.Value{}
	}
}

func TestBlockFastPaths(t *testing.T) {
	e, cs := newTestEngine(t)
	run(t, e, cs, "RPUSH", "l1", "a", "b", "c")

	// BLPOP immediate: [key, value].
	if v := run(t, e, cs, "BLPOP", "l1", "0.05"); v.Kind != resp.KindArray ||
		len(v.Arr) != 2 || string(v.Arr[0].Blob) != "l1" || string(v.Arr[1].Blob) != "a" {
		t.Fatalf("BLPOP = %+v", v)
	}
	// BRPOP immediate from the tail.
	if v := run(t, e, cs, "BRPOP", "l1", "0.05"); v.Kind != resp.KindArray ||
		len(v.Arr) != 2 || string(v.Arr[1].Blob) != "c" {
		t.Fatalf("BRPOP = %+v", v)
	}
	// The list still has "b"; draining it deletes the key.
	if v := run(t, e, cs, "EXISTS", "l1"); v.Int != 1 {
		t.Fatalf("EXISTS after two pops = %+v", v)
	}
	run(t, e, cs, "BLPOP", "l1", "0.05")
	if v := run(t, e, cs, "EXISTS", "l1"); v.Int != 0 {
		t.Fatalf("EXISTS after drain = %+v", v)
	}

	// BZPOPMIN/BZPOPMAX immediate.
	run(t, e, cs, "ZADD", "z1", "1.5", "m1", "2", "m2")
	if v := run(t, e, cs, "BZPOPMIN", "z1", "0.05"); v.Kind != resp.KindArray ||
		len(v.Arr) != 3 || string(v.Arr[1].Blob) != "m1" || string(v.Arr[2].Blob) != "1.5" {
		t.Fatalf("BZPOPMIN RESP2 = %+v", v)
	}
	if v := run(t, e, cs, "BZPOPMAX", "z1", "0.05"); string(v.Arr[1].Blob) != "m2" {
		t.Fatalf("BZPOPMAX = %+v", v)
	}

	// BLMOVE / BRPOPLPUSH immediate.
	run(t, e, cs, "RPUSH", "src", "s1", "s2")
	if v := run(t, e, cs, "BLMOVE", "src", "dst", "LEFT", "RIGHT", "0.05"); string(v.Blob) != "s1" {
		t.Fatalf("BLMOVE = %+v", v)
	}
	if v := run(t, e, cs, "BRPOPLPUSH", "src", "dst", "0.05"); string(v.Blob) != "s2" {
		t.Fatalf("BRPOPLPUSH = %+v", v)
	}
	if v := run(t, e, cs, "LRANGE", "dst", "0", "-1"); len(v.Arr) != 2 ||
		string(v.Arr[0].Blob) != "s2" || string(v.Arr[1].Blob) != "s1" {
		t.Fatalf("dst = %+v", v)
	}

	// BLMPOP immediate with COUNT.
	run(t, e, cs, "RPUSH", "ml", "x", "y", "z")
	v := run(t, e, cs, "BLMPOP", "0.05", "1", "ml", "LEFT", "COUNT", "2")
	if v.Kind != resp.KindArray || len(v.Arr) != 2 || string(v.Arr[0].Blob) != "ml" ||
		len(v.Arr[1].Arr) != 2 || string(v.Arr[1].Arr[0].Blob) != "x" {
		t.Fatalf("BLMPOP = %+v", v)
	}
	// BZMPOP immediate MIN.
	run(t, e, cs, "ZADD", "mz", "1", "a", "2", "b")
	v = run(t, e, cs, "BZMPOP", "0.05", "1", "mz", "MIN")
	if v.Kind != resp.KindArray || len(v.Arr) != 2 || len(v.Arr[1].Arr) != 1 ||
		string(v.Arr[1].Arr[0].Arr[0].Blob) != "a" || string(v.Arr[1].Arr[0].Arr[1].Blob) != "1" {
		t.Fatalf("BZMPOP = %+v", v)
	}
}

func TestBlockTimeoutNulls(t *testing.T) {
	e, cs := newTestEngine(t)
	start := time.Now()
	if v := run(t, e, cs, "BLPOP", "absent", "0.05"); v.Kind != resp.KindNull {
		t.Fatalf("BLPOP timeout = %+v", v)
	}
	if d := time.Since(start); d < 40*time.Millisecond || d > 2*time.Second {
		t.Fatalf("BLPOP timeout took %v", d)
	}
	if v := run(t, e, cs, "BZPOPMIN", "absent", "0.05"); v.Kind != resp.KindNull {
		t.Fatalf("BZPOPMIN timeout = %+v", v)
	}
	if v := run(t, e, cs, "BLMOVE", "absent", "dst", "LEFT", "LEFT", "0.05"); v.Kind != resp.KindNull {
		t.Fatalf("BLMOVE timeout = %+v", v)
	}
	if v := run(t, e, cs, "BLMPOP", "0.05", "1", "absent", "LEFT"); v.Kind != resp.KindNull {
		t.Fatalf("BLMPOP timeout = %+v", v)
	}
	if v := run(t, e, cs, "BZMPOP", "0.05", "1", "absent", "MIN"); v.Kind != resp.KindNull {
		t.Fatalf("BZMPOP timeout = %+v", v)
	}
	if v := run(t, e, cs, "BRPOPLPUSH", "absent", "dst", "0.05"); v.Kind != resp.KindNull {
		t.Fatalf("BRPOPLPUSH timeout = %+v", v)
	}
	// Nothing left parked, nothing left registered.
	if n := e.blockedClients.Load(); n != 0 {
		t.Fatalf("blocked_clients after timeouts = %d", n)
	}
	e.Shards.DoShard(e.Shards.ShardIndex([]byte("absent")), func(s *shard.Shard) {
		// exercised indirectly: no waiters should remain for "absent"
		s.WakeWaiter(cs.DB, "absent")
	})
}

func TestBlockWakeFromPush(t *testing.T) {
	e, cs := newTestEngine(t)
	cs2 := e.NewConnState("127.0.0.1:2")

	res := runBlocked(e, cs, "BLPOP", "wk", "2")
	awaitBlocked(t, e, 1)
	run(t, e, cs2, "LPUSH", "wk", "hello")
	v := recvReply(t, res)
	if v.Kind != resp.KindArray || len(v.Arr) != 2 ||
		string(v.Arr[0].Blob) != "wk" || string(v.Arr[1].Blob) != "hello" {
		t.Fatalf("woken BLPOP = %+v", v)
	}
	awaitBlocked(t, e, 0)

	// Two parked waiters, two pushed elements: both served.
	r1 := runBlocked(e, cs, "BLPOP", "wk2", "2")
	r2 := runBlocked(e, cs2, "BRPOP", "wk2", "2")
	awaitBlocked(t, e, 2)
	run(t, e, cs, "RPUSH", "wk2", "e1", "e2")
	v1, v2 := recvReply(t, r1), recvReply(t, r2)
	// BLPOP takes the head, BRPOP the tail.
	if string(v1.Arr[1].Blob) != "e1" || string(v2.Arr[1].Blob) != "e2" {
		t.Fatalf("two waiters = %+v / %+v", v1, v2)
	}

	// BZPOPMIN woken by ZADD; BLMOVE woken by RPUSH.
	rz := runBlocked(e, cs, "BZPOPMIN", "zk", "2")
	awaitBlocked(t, e, 1)
	run(t, e, cs2, "ZADD", "zk", "7", "zm")
	v = recvReply(t, rz)
	if len(v.Arr) != 3 || string(v.Arr[1].Blob) != "zm" || string(v.Arr[2].Blob) != "7" {
		t.Fatalf("woken BZPOPMIN = %+v", v)
	}
	rm := runBlocked(e, cs, "BLMOVE", "msrc", "mdst", "LEFT", "LEFT", "2")
	awaitBlocked(t, e, 1)
	run(t, e, cs2, "RPUSH", "msrc", "mv")
	v = recvReply(t, rm)
	if string(v.Blob) != "mv" {
		t.Fatalf("woken BLMOVE = %+v", v)
	}
	if v := run(t, e, cs, "GET", "nope"); v.Kind != resp.KindNull { // sanity
		t.Fatalf("GET = %+v", v)
	}
	if v := run(t, e, cs, "LRANGE", "mdst", "0", "-1"); len(v.Arr) != 1 || string(v.Arr[0].Blob) != "mv" {
		t.Fatalf("mdst = %+v", v)
	}
}

func TestBlockMultiKeyPriority(t *testing.T) {
	e, cs := newTestEngine(t)
	run(t, e, cs, "RPUSH", "p1", "v1")
	run(t, e, cs, "RPUSH", "p2", "v2")

	// Both non-empty: the first key wins.
	if v := run(t, e, cs, "BLPOP", "p1", "p2", "0.05"); string(v.Arr[0].Blob) != "p1" {
		t.Fatalf("BLPOP p1 p2 = %+v", v)
	}
	// First empty (drained), second non-empty: second served.
	if v := run(t, e, cs, "BLPOP", "p1", "p2", "0.05"); string(v.Arr[0].Blob) != "p2" {
		t.Fatalf("BLPOP p1 p2 after drain = %+v", v)
	}
	// Same for BZPOPMIN.
	run(t, e, cs, "ZADD", "zp2", "1", "z2")
	if v := run(t, e, cs, "BZPOPMIN", "zp1", "zp2", "0.05"); string(v.Arr[0].Blob) != "zp2" {
		t.Fatalf("BZPOPMIN zp1 zp2 = %+v", v)
	}
}

func TestBlockWrongTypeOrdering(t *testing.T) {
	e, cs := newTestEngine(t)
	// Probed against Redis 7.2.7: keys scan in order; a wrong-type key
	// errors only when reached before any poppable key.
	run(t, e, cs, "SET", "str", "s")
	run(t, e, cs, "RPUSH", "li", "a")

	if v := run(t, e, cs, "BLPOP", "str", "li", "0.05"); v.Kind != resp.KindError ||
		!strings.HasPrefix(v.Str, "WRONGTYPE") {
		t.Fatalf("BLPOP str li = %+v", v)
	}
	if v := run(t, e, cs, "BLPOP", "li", "str", "0.05"); v.Kind != resp.KindArray ||
		string(v.Arr[0].Blob) != "li" {
		t.Fatalf("BLPOP li str = %+v", v)
	}
	// Missing key first, wrong-type second: error (no blocking).
	if v := run(t, e, cs, "BLPOP", "gone", "str", "0.05"); v.Kind != resp.KindError ||
		!strings.HasPrefix(v.Str, "WRONGTYPE") {
		t.Fatalf("BLPOP gone str = %+v", v)
	}
	if v := run(t, e, cs, "BLMPOP", "0.05", "2", "gone", "str", "LEFT"); v.Kind != resp.KindError {
		t.Fatalf("BLMPOP gone str = %+v", v)
	}
	if v := run(t, e, cs, "BZPOPMIN", "str", "0.05"); v.Kind != resp.KindError {
		t.Fatalf("BZPOPMIN str = %+v", v)
	}
	// BLMOVE: wrong-type source errors; wrong-type destination with a
	// MISSING source does not — the command blocks (probed).
	if v := run(t, e, cs, "BLMOVE", "str", "li", "LEFT", "LEFT", "0.05"); v.Kind != resp.KindError {
		t.Fatalf("BLMOVE str li = %+v", v)
	}
	run(t, e, cs, "RPUSH", "li", "b")
	if v := run(t, e, cs, "BLMOVE", "li", "str", "LEFT", "LEFT", "0.05"); v.Kind != resp.KindError {
		t.Fatalf("BLMOVE li str(dst) = %+v", v)
	}
	if v := run(t, e, cs, "BLMOVE", "gone", "str", "LEFT", "LEFT", "0.05"); v.Kind != resp.KindNull {
		t.Fatalf("BLMOVE gone str(dst) = %+v, want null after timeout", v)
	}
}

func TestBlockBadTimeouts(t *testing.T) {
	e, cs := newTestEngine(t)
	cases := []struct{ arg, want string }{
		{"abc", "ERR timeout is not a float or out of range"},
		{"nan", "ERR timeout is not a float or out of range"},
		{"-1", "ERR timeout is negative"},
		{"-0.5", "ERR timeout is negative"},
		{"inf", "ERR timeout is out of range"},
	}
	for _, tc := range cases {
		if v := run(t, e, cs, "BLPOP", "tk", tc.arg); v.Kind != resp.KindError || v.Str != tc.want {
			t.Errorf("BLPOP tk %s = %+v, want %q", tc.arg, v, tc.want)
		}
		if v := run(t, e, cs, "BZPOPMAX", "tk", tc.arg); v.Kind != resp.KindError || v.Str != tc.want {
			t.Errorf("BZPOPMAX tk %s = %+v, want %q", tc.arg, v, tc.want)
		}
	}
	// "-0" blocks forever (probed): park, then wake.
	res := runBlocked(e, cs, "BLPOP", "tk", "-0")
	awaitBlocked(t, e, 1)
	run(t, e, cs, "LPUSH", "tk", "zv")
	if v := recvReply(t, res); string(v.Arr[1].Blob) != "zv" {
		t.Fatalf("BLPOP -0 wake = %+v", v)
	}
	// BLMOVE/BRPOPLPUSH bad timeout too.
	if v := run(t, e, cs, "BLMOVE", "a", "b", "LEFT", "LEFT", "-1"); v.Str != "ERR timeout is negative" {
		t.Fatalf("BLMOVE -1 = %+v", v)
	}
	if v := run(t, e, cs, "BRPOPLPUSH", "a", "b", "abc"); v.Str != "ERR timeout is not a float or out of range" {
		t.Fatalf("BRPOPLPUSH abc = %+v", v)
	}
}

func TestBLMoveDirections(t *testing.T) {
	e, cs := newTestEngine(t)
	for _, tc := range []struct {
		src, dst, want string
	}{
		{"LEFT", "LEFT", "a"},
		{"LEFT", "RIGHT", "a"},
		{"RIGHT", "LEFT", "d"},
		{"RIGHT", "RIGHT", "d"},
	} {
		run(t, e, cs, "DEL", "ms", "md")
		run(t, e, cs, "RPUSH", "ms", "a", "b", "c", "d")
		if v := run(t, e, cs, "BLMOVE", "ms", "md", tc.src, tc.dst, "0.05"); string(v.Blob) != tc.want {
			t.Errorf("BLMOVE %s %s = %+v, want %q", tc.src, tc.dst, v, tc.want)
		}
	}
	// Destination side: LEFT pushes to the head, RIGHT to the tail.
	run(t, e, cs, "DEL", "md")
	run(t, e, cs, "RPUSH", "md", "m")
	run(t, e, cs, "RPUSH", "ms2", "h", "t")
	run(t, e, cs, "BLMOVE", "ms2", "md", "LEFT", "LEFT", "0.05")
	run(t, e, cs, "BLMOVE", "ms2", "md", "RIGHT", "RIGHT", "0.05")
	if v := run(t, e, cs, "LRANGE", "md", "0", "-1"); len(v.Arr) != 3 ||
		string(v.Arr[0].Blob) != "h" || string(v.Arr[1].Blob) != "m" || string(v.Arr[2].Blob) != "t" {
		t.Errorf("md after direction moves = %+v", v)
	}
	if v := run(t, e, cs, "BLMOVE", "ms", "md", "UP", "LEFT", "0.05"); v.Str != "ERR syntax error" {
		t.Fatalf("BLMOVE bad src dir = %+v", v)
	}
	if v := run(t, e, cs, "BLMOVE", "ms", "md", "LEFT", "UP", "0.05"); v.Str != "ERR syntax error" {
		t.Fatalf("BLMOVE bad dst dir = %+v", v)
	}
	// Same-key move.
	run(t, e, cs, "DEL", "same")
	run(t, e, cs, "RPUSH", "same", "1", "2")
	if v := run(t, e, cs, "BLMOVE", "same", "same", "LEFT", "RIGHT", "0.05"); string(v.Blob) != "1" {
		t.Fatalf("BLMOVE same-key = %+v", v)
	}
}

func TestMPopParseErrors(t *testing.T) {
	e, cs := newTestEngine(t)
	cases := []struct {
		args []string
		want string
	}{
		{[]string{"BLMPOP", "0", "0", "l", "LEFT"}, "ERR numkeys should be greater than 0"},
		{[]string{"BLMPOP", "0", "-1", "l", "LEFT"}, "ERR numkeys should be greater than 0"},
		{[]string{"BLMPOP", "0", "abc", "l", "LEFT"}, "ERR numkeys should be greater than 0"},
		{[]string{"BLMPOP", "0", "2", "l", "LEFT"}, "ERR syntax error"}, // numkeys eats the direction
		{[]string{"BLMPOP", "0", "1", "l", "UP"}, "ERR syntax error"},
		{[]string{"BLMPOP", "0", "1", "l", "LEFT", "JUNK"}, "ERR syntax error"},
		{[]string{"BLMPOP", "0", "1", "l", "LEFT", "COUNT"}, "ERR syntax error"},
		{[]string{"BLMPOP", "0", "1", "l", "LEFT", "COUNT", "0"}, "ERR count should be greater than 0"},
		{[]string{"BLMPOP", "0", "1", "l", "LEFT", "COUNT", "-1"}, "ERR count should be greater than 0"},
		{[]string{"BLMPOP", "0", "1", "l", "LEFT", "COUNT", "abc"}, "ERR count should be greater than 0"},
		{[]string{"BLMPOP", "abc", "1", "l", "LEFT"}, "ERR timeout is not a float or out of range"},
		{[]string{"BZMPOP", "0", "0", "z", "MIN"}, "ERR numkeys should be greater than 0"},
		{[]string{"BZMPOP", "0", "1", "z", "SIDEWAYS"}, "ERR syntax error"},
		{[]string{"BZMPOP", "0", "1", "z", "MIN", "COUNT", "0"}, "ERR count should be greater than 0"},
		{[]string{"BZMPOP", "0", "1", "z", "MIN", "JUNK"}, "ERR syntax error"},
	}
	for _, tc := range cases {
		if v := run(t, e, cs, tc.args...); v.Kind != resp.KindError || v.Str != tc.want {
			t.Errorf("%v = %+v, want %q", tc.args, v, tc.want)
		}
	}
}

func TestBZPopProtoShapes(t *testing.T) {
	e, cs := newTestEngine(t)
	run(t, e, cs, "ZADD", "pz", "1.5", "m1", "2", "m2")

	// RESP2: score is a bulk string via FormatDouble.
	v := run(t, e, cs, "BZPOPMIN", "pz", "0.05")
	if v.Arr[2].Kind != resp.KindBlobString || string(v.Arr[2].Blob) != "1.5" {
		t.Fatalf("BZPOPMIN RESP2 score = %+v", v.Arr[2])
	}
	// RESP3: score is a double.
	run(t, e, cs, "HELLO", "3")
	v = run(t, e, cs, "BZPOPMAX", "pz", "0.05")
	if v.Arr[2].Kind != resp.KindDouble || v.Arr[2].Dbl != 2 {
		t.Fatalf("BZPOPMIN RESP3 score = %+v", v.Arr[2])
	}
	// BZMPOP RESP3: [key, [[member, double]...]].
	run(t, e, cs, "ZADD", "pz2", "3", "n1")
	v = run(t, e, cs, "BZMPOP", "0.05", "1", "pz2", "MAX")
	if len(v.Arr) != 2 || len(v.Arr[1].Arr) != 1 ||
		v.Arr[1].Arr[0].Arr[1].Kind != resp.KindDouble || v.Arr[1].Arr[0].Arr[1].Dbl != 3 {
		t.Fatalf("BZMPOP RESP3 = %+v", v)
	}
	// RESP3 null is a plain null.
	if v := run(t, e, cs, "BLPOP", "gone", "0.05"); v.Kind != resp.KindNull {
		t.Fatalf("RESP3 BLPOP null = %+v", v)
	}
}

func TestBlockInExec(t *testing.T) {
	e, cs := newTestEngine(t)
	// Blocking commands inside MULTI take the non-blocking fast path:
	// queued BLPOP/BZPOPMIN on missing keys EXEC to null elements.
	run(t, e, cs, "MULTI")
	if v := run(t, e, cs, "BLPOP", "mk", "5"); v.Str != "QUEUED" {
		t.Fatalf("queue BLPOP = %+v", v)
	}
	run(t, e, cs, "BZPOPMIN", "mz", "5")
	start := time.Now()
	v := run(t, e, cs, "EXEC")
	if d := time.Since(start); d > time.Second {
		t.Fatalf("EXEC blocked for %v", d)
	}
	if v.Kind != resp.KindArray || len(v.Arr) != 2 ||
		v.Arr[0].Kind != resp.KindNull || v.Arr[1].Kind != resp.KindNull {
		t.Fatalf("EXEC = %+v", v)
	}
	// With data present the EXEC pops immediately.
	run(t, e, cs, "RPUSH", "mk", "e1")
	run(t, e, cs, "MULTI")
	run(t, e, cs, "BLPOP", "mk", "5")
	v = run(t, e, cs, "EXEC")
	if len(v.Arr) != 1 || v.Arr[0].Kind != resp.KindArray ||
		string(v.Arr[0].Arr[1].Blob) != "e1" {
		t.Fatalf("EXEC with data = %+v", v)
	}
}

func TestBlockInfoBlockedClients(t *testing.T) {
	e, cs := newTestEngine(t)
	res := runBlocked(e, cs, "BLPOP", "ik", "2")
	awaitBlocked(t, e, 1)
	v := run(t, e, cs, "INFO", "clients")
	if !strings.Contains(string(v.Blob), "blocked_clients:1\r\n") {
		t.Fatalf("INFO clients = %q", string(v.Blob))
	}
	run(t, e, cs, "LPUSH", "ik", "v")
	recvReply(t, res)
	awaitBlocked(t, e, 0)
	v = run(t, e, cs, "INFO", "clients")
	if !strings.Contains(string(v.Blob), "blocked_clients:0\r\n") {
		t.Fatalf("INFO clients after wake = %q", string(v.Blob))
	}
}

func TestBlockEngineCloseUnparks(t *testing.T) {
	sh := shard.NewEngine(4, 16)
	e := NewEngine(sh, "test", 6379)
	cs := e.NewConnState("127.0.0.1:1")

	res := runBlocked(e, cs, "BLPOP", "ck", "0") // parked forever
	awaitBlocked(t, e, 1)
	closed := make(chan struct{})
	go func() { sh.Close(); close(closed) }()
	select {
	case v := <-res:
		if v.Kind != resp.KindNull {
			t.Fatalf("parked BLPOP on Close = %+v, want null", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked BLPOP did not resolve on engine Close")
	}
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("engine Close hung with a parked waiter")
	}
}
