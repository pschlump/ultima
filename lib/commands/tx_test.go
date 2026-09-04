package commands

import (
	"fmt"
	"testing"

	"github.com/pschlump/ultima/lib/resp"
)

func expectSimple(t *testing.T, v resp.Value, want string) {
	t.Helper()
	if v.Kind != resp.KindSimpleString || v.Str != want {
		t.Fatalf("got %+v, want +%s", v, want)
	}
}

func expectErr(t *testing.T, v resp.Value, want string) {
	t.Helper()
	if v.Kind != resp.KindError || v.Str != want {
		t.Fatalf("got %+v, want error %q", v, want)
	}
}

func expectNull(t *testing.T, v resp.Value) {
	t.Helper()
	if v.Kind != resp.KindNull {
		t.Fatalf("got %+v, want null", v)
	}
}

func expectBlob(t *testing.T, v resp.Value, want string) {
	t.Helper()
	if v.Kind != resp.KindBlobString || string(v.Blob) != want {
		t.Fatalf("got %+v, want $%q", v, want)
	}
}

func TestMultiExecBasic(t *testing.T) {
	e, cs := newTestEngine(t)
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectSimple(t, run(t, e, cs, "SET", "a", "1"), "QUEUED")
	expectSimple(t, run(t, e, cs, "INCR", "a"), "QUEUED")
	v := run(t, e, cs, "EXEC")
	if v.Kind != resp.KindArray || len(v.Arr) != 2 {
		t.Fatalf("EXEC = %+v", v)
	}
	expectSimple(t, v.Arr[0], "OK")
	if v.Arr[1].Kind != resp.KindInt || v.Arr[1].Int != 2 {
		t.Fatalf("EXEC[1] = %+v", v.Arr[1])
	}
	expectBlob(t, run(t, e, cs, "GET", "a"), "2")
	// EXEC left multi mode behind
	expectErr(t, run(t, e, cs, "EXEC"), "ERR EXEC without MULTI")
	// an empty queue commits as an empty array
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	if v := run(t, e, cs, "EXEC"); v.Kind != resp.KindArray || len(v.Arr) != 0 {
		t.Fatalf("EXEC empty = %+v", v)
	}
}

func TestExecDiscardWithoutMulti(t *testing.T) {
	e, cs := newTestEngine(t)
	expectErr(t, run(t, e, cs, "EXEC"), "ERR EXEC without MULTI")
	expectErr(t, run(t, e, cs, "DISCARD"), "ERR DISCARD without MULTI")
}

func TestNestedMulti(t *testing.T) {
	e, cs := newTestEngine(t)
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectErr(t, run(t, e, cs, "MULTI"), "ERR MULTI calls can not be nested")
	// nested-MULTI error does not dirty the transaction
	expectSimple(t, run(t, e, cs, "SET", "n", "1"), "QUEUED")
	v := run(t, e, cs, "EXEC")
	if v.Kind != resp.KindArray || len(v.Arr) != 1 || v.Arr[0].Str != "OK" {
		t.Fatalf("EXEC = %+v", v)
	}
	expectBlob(t, run(t, e, cs, "GET", "n"), "1")
}

func TestExecAbortUnknownCommand(t *testing.T) {
	e, cs := newTestEngine(t)
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectErr(t, run(t, e, cs, "NOSUCHCMD", "x"),
		"ERR unknown command 'NOSUCHCMD', with args beginning with: 'x' ")
	expectSimple(t, run(t, e, cs, "SET", "k", "v"), "QUEUED")
	expectErr(t, run(t, e, cs, "EXEC"),
		"EXECABORT Transaction discarded because of previous errors.")
	// nothing applied, state fully cleared
	expectNull(t, run(t, e, cs, "GET", "k"))
	expectErr(t, run(t, e, cs, "EXEC"), "ERR EXEC without MULTI")
}

func TestExecAbortArity(t *testing.T) {
	e, cs := newTestEngine(t)
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectErr(t, run(t, e, cs, "GET", "a", "b"),
		"ERR wrong number of arguments for 'get' command")
	expectErr(t, run(t, e, cs, "EXEC"),
		"EXECABORT Transaction discarded because of previous errors.")
	expectErr(t, run(t, e, cs, "EXEC"), "ERR EXEC without MULTI")
}

func TestExecErrorElement(t *testing.T) {
	e, cs := newTestEngine(t)
	run(t, e, cs, "SET", "bad", "abc")
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectSimple(t, run(t, e, cs, "INCR", "bad"), "QUEUED")
	expectSimple(t, run(t, e, cs, "SET", "after", "1"), "QUEUED")
	v := run(t, e, cs, "EXEC")
	if v.Kind != resp.KindArray || len(v.Arr) != 2 {
		t.Fatalf("EXEC = %+v", v)
	}
	expectErr(t, v.Arr[0], "ERR value is not an integer or out of range")
	expectSimple(t, v.Arr[1], "OK")
	// execution continued past the error
	expectBlob(t, run(t, e, cs, "GET", "after"), "1")
}

func TestWatchCleanExecApplies(t *testing.T) {
	e, cs := newTestEngine(t)
	run(t, e, cs, "SET", "w", "orig")
	expectSimple(t, run(t, e, cs, "WATCH", "w", "missing"), "OK")
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectSimple(t, run(t, e, cs, "SET", "w", "mine"), "QUEUED")
	v := run(t, e, cs, "EXEC")
	if v.Kind != resp.KindArray || len(v.Arr) != 1 || v.Arr[0].Str != "OK" {
		t.Fatalf("EXEC = %+v", v)
	}
	expectBlob(t, run(t, e, cs, "GET", "w"), "mine")
}

// dirtyWatch runs the WATCH/MULTI/SET/EXEC dance around fn's interference
// from a second connection and asserts EXEC returns the null array and
// applies nothing. wantW is the value "w" must hold afterwards (the
// interfering write, untouched by the aborted EXEC); "" means the key is
// gone.
func dirtyWatch(t *testing.T, fn func(t *testing.T, e *Engine, other *ConnState), wantW string) {
	t.Helper()
	e, cs := newTestEngine(t)
	other := e.NewConnState("127.0.0.1:2")
	run(t, e, cs, "SET", "w", "1")
	expectSimple(t, run(t, e, cs, "WATCH", "w"), "OK")
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectSimple(t, run(t, e, cs, "SET", "w", "mine"), "QUEUED")
	expectSimple(t, run(t, e, cs, "SET", "otherkey", "mine"), "QUEUED")
	fn(t, e, other)
	expectNull(t, run(t, e, cs, "EXEC"))
	// nothing from the queue applied; w holds the interfering write
	if wantW == "" {
		expectNull(t, run(t, e, cs, "GET", "w"))
	} else {
		expectBlob(t, run(t, e, cs, "GET", "w"), wantW)
	}
	expectNull(t, run(t, e, cs, "GET", "otherkey"))
	// state fully cleared
	expectErr(t, run(t, e, cs, "EXEC"), "ERR EXEC without MULTI")
}

func TestWatchDirtyStore(t *testing.T) {
	dirtyWatch(t, func(t *testing.T, e *Engine, other *ConnState) {
		run(t, e, other, "SET", "w", "2") // Store path
	}, "2")
}

func TestWatchDirtyTouch(t *testing.T) {
	dirtyWatch(t, func(t *testing.T, e *Engine, other *ConnState) {
		run(t, e, other, "INCR", "w") // in-place Touch path
	}, "2")
}

func TestWatchDirtyDel(t *testing.T) {
	dirtyWatch(t, func(t *testing.T, e *Engine, other *ConnState) {
		run(t, e, other, "DEL", "w")
	}, "")
}

func TestWatchDirtyFlushDB(t *testing.T) {
	dirtyWatch(t, func(t *testing.T, e *Engine, other *ConnState) {
		run(t, e, other, "FLUSHDB")
	}, "")
}

func TestUnwatchClears(t *testing.T) {
	e, cs := newTestEngine(t)
	other := e.NewConnState("127.0.0.1:2")
	run(t, e, cs, "SET", "w", "1")
	expectSimple(t, run(t, e, cs, "WATCH", "w"), "OK")
	expectSimple(t, run(t, e, cs, "UNWATCH"), "OK")
	run(t, e, other, "SET", "w", "2") // no longer watched
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectSimple(t, run(t, e, cs, "SET", "w", "mine"), "QUEUED")
	if v := run(t, e, cs, "EXEC"); v.Kind != resp.KindArray {
		t.Fatalf("EXEC = %+v, want array", v)
	}
	expectBlob(t, run(t, e, cs, "GET", "w"), "mine")
}

func TestUnwatchQueuedInsideMulti(t *testing.T) {
	e, cs := newTestEngine(t)
	expectSimple(t, run(t, e, cs, "WATCH", "w"), "OK")
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectSimple(t, run(t, e, cs, "UNWATCH"), "QUEUED")
	v := run(t, e, cs, "EXEC")
	if v.Kind != resp.KindArray || len(v.Arr) != 1 || v.Arr[0].Str != "OK" {
		t.Fatalf("EXEC = %+v", v)
	}
}

func TestDiscardDiscards(t *testing.T) {
	e, cs := newTestEngine(t)
	expectSimple(t, run(t, e, cs, "WATCH", "w"), "OK")
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectSimple(t, run(t, e, cs, "SET", "d", "1"), "QUEUED")
	expectSimple(t, run(t, e, cs, "DISCARD"), "OK")
	expectNull(t, run(t, e, cs, "GET", "d"))
	// DISCARD also dropped the watches: touching w must not abort EXEC
	other := e.NewConnState("127.0.0.1:2")
	run(t, e, other, "SET", "w", "2")
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectSimple(t, run(t, e, cs, "SET", "d", "1"), "QUEUED")
	if v := run(t, e, cs, "EXEC"); v.Kind != resp.KindArray {
		t.Fatalf("EXEC = %+v, want array", v)
	}
	expectBlob(t, run(t, e, cs, "GET", "d"), "1")
}

func TestWatchInsideMulti(t *testing.T) {
	e, cs := newTestEngine(t)
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectErr(t, run(t, e, cs, "WATCH", "x"), "ERR WATCH inside MULTI is not allowed")
	// the error does not dirty the transaction
	expectSimple(t, run(t, e, cs, "SET", "k", "v"), "QUEUED")
	if v := run(t, e, cs, "EXEC"); v.Kind != resp.KindArray || len(v.Arr) != 1 {
		t.Fatalf("EXEC = %+v", v)
	}
}

func TestExecCrossShard(t *testing.T) {
	e, cs := newTestEngine(t)
	// find two keys routed to different shards (test engine has 4); the
	// crc64 low bits that ShardIndex reads are content-weak for short or
	// decimal-suffixed keys, so spread the hash input multiplicatively
	k1 := fmt.Sprintf("key%012x", 0)
	k2 := ""
	for i := 1; i < 1000; i++ {
		k := fmt.Sprintf("key%012x", i*2654435761)
		if e.Shards.ShardIndex([]byte(k)) != e.Shards.ShardIndex([]byte(k1)) {
			k2 = k
			break
		}
	}
	if k2 == "" {
		t.Fatal("no cross-shard key found")
	}
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectSimple(t, run(t, e, cs, "MSET", k1, "v1", k2, "v2"), "QUEUED")
	expectSimple(t, run(t, e, cs, "MGET", k1, k2), "QUEUED")
	v := run(t, e, cs, "EXEC")
	if v.Kind != resp.KindArray || len(v.Arr) != 2 {
		t.Fatalf("EXEC = %+v", v)
	}
	expectSimple(t, v.Arr[0], "OK")
	if v.Arr[1].Kind != resp.KindArray || len(v.Arr[1].Arr) != 2 {
		t.Fatalf("EXEC[1] = %+v", v.Arr[1])
	}
	expectBlob(t, v.Arr[1].Arr[0], "v1")
	expectBlob(t, v.Arr[1].Arr[1], "v2")
	expectBlob(t, run(t, e, cs, "GET", k1), "v1")
	expectBlob(t, run(t, e, cs, "GET", k2), "v2")
}

func TestReset(t *testing.T) {
	e, cs := newTestEngine(t)
	run(t, e, cs, "CLIENT", "SETNAME", "before")
	run(t, e, cs, "SELECT", "1")
	run(t, e, cs, "SET", "rk", "in-db1")
	run(t, e, cs, "HELLO", "3")
	expectSimple(t, run(t, e, cs, "WATCH", "rk"), "OK")
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectSimple(t, run(t, e, cs, "SET", "queued", "1"), "QUEUED")
	expectSimple(t, run(t, e, cs, "RESET"), "RESET")
	// multi state gone (queued command never ran)
	expectErr(t, run(t, e, cs, "EXEC"), "ERR EXEC without MULTI")
	expectNull(t, run(t, e, cs, "GET", "queued"))
	// back on DB 0, RESP2, no name
	if cs.DB != 0 || cs.Proto != 2 || cs.Name != "" {
		t.Fatalf("cs after RESET: db=%d proto=%d name=%q", cs.DB, cs.Proto, cs.Name)
	}
	expectNull(t, run(t, e, cs, "CLIENT", "GETNAME"))
	expectNull(t, run(t, e, cs, "GET", "rk")) // rk lives in db1
	run(t, e, cs, "SELECT", "1")
	expectBlob(t, run(t, e, cs, "GET", "rk"), "in-db1")
	// watches dropped: touching rk must not abort a later EXEC
	other := e.NewConnState("127.0.0.1:2")
	other.DB = 1
	run(t, e, other, "SET", "rk", "touched")
	run(t, e, cs, "MULTI")
	expectSimple(t, run(t, e, cs, "SET", "rk", "final"), "QUEUED")
	if v := run(t, e, cs, "EXEC"); v.Kind != resp.KindArray {
		t.Fatalf("EXEC after RESET = %+v, want array", v)
	}
	// RESET with arguments is an arity error
	expectErr(t, run(t, e, cs, "RESET", "x"),
		"ERR wrong number of arguments for 'reset' command")
}

func TestResetClearsAuth(t *testing.T) {
	e, cs := newTestEngine(t)
	e.SetRequirePass("pw")
	expectErr(t, run(t, e, cs, "GET", "x"), "NOAUTH Authentication required.")
	expectSimple(t, run(t, e, cs, "AUTH", "pw"), "OK")
	expectSimple(t, run(t, e, cs, "RESET"), "RESET")
	// RESET de-authenticates (verified against Redis 7.2.7)
	expectErr(t, run(t, e, cs, "GET", "x"), "NOAUTH Authentication required.")
	// RESET itself is allowed unauthenticated
	expectSimple(t, run(t, e, cs, "RESET"), "RESET")
}
