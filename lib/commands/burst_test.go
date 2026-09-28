package commands

// M9b burst-coalescing tests (lib/commands/burst.go): segment grouping,
// the inline do() path, segment breaks (non-whitelisted commands, MULTI
// queueing, malformed argv), the maxmemory disable, and reply ordering.

import (
	"testing"

	"github.com/pschlump/ultima/lib/resp"
)

// burst builds firstArgs + rest for ExecuteBurst (only Args is consulted).
func burst(cmds ...[]string) ([][]byte, []resp.Command) {
	toArgs := func(ss []string) [][]byte {
		bb := make([][]byte, len(ss))
		for i, s := range ss {
			bb[i] = []byte(s)
		}
		return bb
	}
	first := toArgs(cmds[0])
	rest := make([]resp.Command, 0, len(cmds)-1)
	for _, c := range cmds[1:] {
		rest = append(rest, resp.Command{Args: toArgs(c)})
	}
	return first, rest
}

func runBurst(t *testing.T, e *Engine, cs *ConnState, cmds ...[]string) []resp.Value {
	t.Helper()
	first, rest := burst(cmds...)
	vals, n := e.ExecuteBurst(cs, first, rest)
	if n != len(rest) {
		t.Fatalf("ExecuteBurst consumed %d of %d rest commands", n, len(rest))
	}
	if len(vals) != len(cmds) {
		t.Fatalf("ExecuteBurst returned %d replies for %d commands: %+v", len(vals), len(cmds), vals)
	}
	return vals
}

// keyOnShard returns a key (suffix-varying) that routes to a different
// shard than base.
func keyOnShard(e *Engine, base string) string {
	want := e.Shards.ShardIndex([]byte(base))
	for i := 0; ; i++ {
		k := base + "#" + string(rune('a'+i))
		if e.Shards.ShardIndex([]byte(k)) != want {
			return k
		}
	}
}

func TestBurstSameKeyOrderWithinSegment(t *testing.T) {
	e, cs := newTestEngine(t)
	// Same-key commands in one segment must observe each other's effects
	// in issue order (per-shard group preserves order).
	vals := runBurst(t, e, cs,
		[]string{"SET", "k", "v1"},
		[]string{"SET", "k", "v2"},
		[]string{"APPEND", "k", "3"},
		[]string{"GET", "k"},
		[]string{"STRLEN", "k"},
	)
	expectSimple(t, vals[0], "OK")
	expectSimple(t, vals[1], "OK")
	if vals[2].Kind != resp.KindInt || vals[2].Int != 3 {
		t.Fatalf("APPEND reply = %+v, want :3", vals[2])
	}
	expectBlob(t, vals[3], "v23")
	if vals[4].Kind != resp.KindInt || vals[4].Int != 3 {
		t.Fatalf("STRLEN reply = %+v, want :3", vals[4])
	}
}

func TestBurstCrossShardSegment(t *testing.T) {
	e, cs := newTestEngine(t)
	k2 := keyOnShard(e, "k") // different shard than "k", same segment
	vals := runBurst(t, e, cs,
		[]string{"SET", "k", "a"},
		[]string{"SET", k2, "b"},
		[]string{"INCRBY", "n", "5"},
		[]string{"GET", "k"},
		[]string{"GET", k2},
		[]string{"GET", "n"},
	)
	expectSimple(t, vals[0], "OK")
	expectSimple(t, vals[1], "OK")
	if vals[2].Kind != resp.KindInt || vals[2].Int != 5 {
		t.Fatalf("INCRBY reply = %+v, want :5", vals[2])
	}
	expectBlob(t, vals[3], "a")
	expectBlob(t, vals[4], "b")
	expectBlob(t, vals[5], "5")
}

func TestBurstSegmentBreakOnNonWhitelisted(t *testing.T) {
	e, cs := newTestEngine(t)
	// PING breaks the segment: everything still executes in issue order.
	vals := runBurst(t, e, cs,
		[]string{"SET", "k", "v"},
		[]string{"PING"},
		[]string{"GET", "k"},
	)
	expectSimple(t, vals[0], "OK")
	expectSimple(t, vals[1], "PONG")
	expectBlob(t, vals[2], "v")
}

func TestBurstMalformedArgsBreaksSegment(t *testing.T) {
	e, cs := newTestEngine(t)
	// GET with no key is not burst-eligible (< 2 args): it takes the
	// normal path and reports arity; the following GET still runs.
	vals := runBurst(t, e, cs,
		[]string{"SET", "k", "v"},
		[]string{"GET"},
		[]string{"GET", "k"},
	)
	expectSimple(t, vals[0], "OK")
	expectErr(t, vals[1], "ERR wrong number of arguments for 'get' command")
	expectBlob(t, vals[2], "v")
}

func TestBurstMultiQueueing(t *testing.T) {
	e, cs := newTestEngine(t)
	// A MULTI inside the burst flips cs.Multi for the rest of the burst:
	// whitelisted commands after it must queue, not execute.
	vals := runBurst(t, e, cs,
		[]string{"SET", "n", "0"},
		[]string{"MULTI"},
		[]string{"INCR", "n"},
		[]string{"INCR", "n"},
		[]string{"EXEC"},
		[]string{"GET", "n"},
	)
	expectSimple(t, vals[0], "OK")
	expectSimple(t, vals[1], "OK")
	expectSimple(t, vals[2], "QUEUED")
	expectSimple(t, vals[3], "QUEUED")
	if vals[4].Kind != resp.KindArray || len(vals[4].Arr) != 2 {
		t.Fatalf("EXEC reply = %+v, want 2-element array", vals[4])
	}
	expectBlob(t, vals[5], "2")
}

func TestBurstDisabledWithMaxMemory(t *testing.T) {
	e, cs := newTestEngine(t)
	e.SetMaxMemory(1 << 20)
	first, rest := burst(
		[]string{"SET", "k", "v"},
		[]string{"GET", "k"},
	)
	vals, n := e.ExecuteBurst(cs, first, rest)
	if n != 0 || len(vals) != 1 {
		t.Fatalf("with maxmemory set ExecuteBurst = %d vals, consumed %d; want 1 val, 0 consumed", len(vals), n)
	}
	expectSimple(t, vals[0], "OK")
}

func TestBurstEmptyRest(t *testing.T) {
	e, cs := newTestEngine(t)
	vals, n := e.ExecuteBurst(cs, [][]byte{[]byte("PING")}, nil)
	if n != 0 || len(vals) != 1 {
		t.Fatalf("empty rest = %d vals, consumed %d; want 1 val, 0 consumed", len(vals), n)
	}
	expectSimple(t, vals[0], "PONG")
}

func TestBurstInlineSlotsCleared(t *testing.T) {
	e, cs := newTestEngine(t)
	runBurst(t, e, cs,
		[]string{"SET", "k", "v"},
		[]string{"GET", "k"},
	)
	if cs.inls != nil {
		for i, s := range cs.inls {
			if s != nil {
				t.Fatalf("cs.inls[%d] still set after burst", i)
			}
		}
	}
}

func TestBurstErrorsDoNotAbortSegment(t *testing.T) {
	e, cs := newTestEngine(t)
	// A type error mid-segment is a reply value, not an abort: every
	// command in the segment runs and replies in order.
	if v := run(t, e, cs, "LPUSH", "l", "x"); v.Kind != resp.KindInt || v.Int != 1 {
		t.Fatalf("LPUSH setup = %+v, want :1", v)
	}
	vals := runBurst(t, e, cs,
		[]string{"SET", "k", "v"},
		[]string{"INCR", "l"}, // WRONGTYPE
		[]string{"GET", "k"},
	)
	expectSimple(t, vals[0], "OK")
	if vals[1].Kind != resp.KindError {
		t.Fatalf("INCR on list = %+v, want error", vals[1])
	}
	expectBlob(t, vals[2], "v")
}
