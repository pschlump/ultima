package commands

// M5b OOM-gate tests (Redis processCommand's maxmemory block, probed
// against a live 7.2.7): the denyoom rejection, the MULTI queue-time
// rejection of ANY command while over limit, the EXECABORT forms, the
// eviction-policy reprieve, and CONFIG maxmemory-policy parity.
// Shard-level victim selection is in lib/shard/evict_test.go.

import (
	"strings"
	"testing"

	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
)

const oomErr = "OOM command not allowed when used memory > 'maxmemory'."

// overLimit stores one key and then sets maxmemory below the keyspace's
// estimated bytes, putting the engine over the limit.
func overLimit(e *Engine) {
	e.SetMaxMemory(1) // one stored key (key+value+overhead) is way over 1 byte
}

func TestOOMNoEviction(t *testing.T) {
	e, cs := newTestEngine(t)
	expectSimple(t, run(t, e, cs, "SET", "k", "v"), "OK")
	overLimit(e)

	expectErr(t, run(t, e, cs, "SET", "a", "b"), oomErr)
	expectErr(t, run(t, e, cs, "INCR", "n"), oomErr)
	expectErr(t, run(t, e, cs, "LPUSH", "l", "x"), oomErr)
	// Reads, deletes, expiry and admin commands are not denyoom.
	expectBlob(t, run(t, e, cs, "GET", "k"), "v")
	if v := run(t, e, cs, "EXPIRE", "k", "100"); v.Kind != resp.KindInt || v.Int != 1 {
		t.Fatalf("EXPIRE under OOM = %+v, want :1", v)
	}
	if v := run(t, e, cs, "DEL", "k"); v.Kind != resp.KindInt || v.Int != 1 {
		t.Fatalf("DEL under OOM = %+v, want :1", v)
	}
}

func TestOOMNoEvictionMulti(t *testing.T) {
	e, cs := newTestEngine(t)
	expectSimple(t, run(t, e, cs, "SET", "k", "v"), "OK")
	overLimit(e)

	// While over the limit, EVERY command queued in MULTI is rejected at
	// queue time (even reads) and dirties the transaction (7.2.7 probe).
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectErr(t, run(t, e, cs, "SET", "a", "b"), oomErr)
	expectErr(t, run(t, e, cs, "GET", "k"), oomErr)
	expectErr(t, run(t, e, cs, "EXEC"),
		"EXECABORT Transaction discarded because of previous errors.")

	// MULTI itself and DISCARD are never gated.
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectSimple(t, run(t, e, cs, "DISCARD"), "OK")
}

func TestOOMExecWithQueuedWrite(t *testing.T) {
	e, cs := newTestEngine(t)
	// Queue while under the limit, then go over: EXEC of a transaction
	// containing a denyoom command aborts with the reason embedded.
	expectSimple(t, run(t, e, cs, "SET", "k", "v"), "OK") // keyspace is non-empty
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectSimple(t, run(t, e, cs, "SET", "a", "b"), "QUEUED")
	overLimit(e)
	expectErr(t, run(t, e, cs, "EXEC"),
		"EXECABORT Transaction discarded because of: "+oomErr)
	// The abort discarded the transaction; the write never happened.
	expectNull(t, run(t, e, cs, "GET", "a"))
}

func TestOOMExecReadOnlyRuns(t *testing.T) {
	e, cs := newTestEngine(t)
	expectSimple(t, run(t, e, cs, "SET", "k", "v"), "OK")
	// Queue a read-only transaction while under the limit.
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectSimple(t, run(t, e, cs, "GET", "k"), "QUEUED")
	overLimit(e)
	// No denyoom command inside: EXEC proceeds (7.2.7 probe).
	v := run(t, e, cs, "EXEC")
	if v.Kind != resp.KindArray || len(v.Arr) != 1 ||
		v.Arr[0].Kind != resp.KindBlobString || string(v.Arr[0].Blob) != "v" {
		t.Fatalf("read-only EXEC under OOM = %+v, want [\"v\"]", v)
	}
}

func TestEvictionPolicyReprieve(t *testing.T) {
	e, cs := newTestEngine(t)
	run(t, e, cs, "CONFIG", "SET", "maxmemory-policy", "allkeys-lru")
	expectSimple(t, run(t, e, cs, "SET", "k", "v"), "OK")
	overLimit(e)
	// Over the limit but with an eviction policy and victims: writes
	// proceed, the gate's eviction attempt frees the space.
	expectSimple(t, run(t, e, cs, "SET", "a", "b"), "OK")
	if got := e.Shards.EvictedKeys.Load(); got < 1 {
		t.Error("evicted_keys = 0 after an over-limit write with allkeys-lru")
	}
	// MULTI queueing also proceeds once eviction can keep up.
	expectSimple(t, run(t, e, cs, "MULTI"), "OK")
	expectSimple(t, run(t, e, cs, "GET", "a"), "QUEUED")
	if v := run(t, e, cs, "EXEC"); v.Kind != resp.KindArray {
		t.Fatalf("EXEC under allkeys-lru = %+v, want array", v)
	}
}

func TestVolatileExhaustionOOM(t *testing.T) {
	e, cs := newTestEngine(t)
	// Volatile policy with no TTL'd keys anywhere: eviction cannot help,
	// so an over-limit write is rejected exactly like noeviction.
	run(t, e, cs, "CONFIG", "SET", "maxmemory-policy", "volatile-lru")
	expectSimple(t, run(t, e, cs, "SET", "k", "v"), "OK")
	overLimit(e)
	expectErr(t, run(t, e, cs, "SET", "a", "b"), oomErr)
}

func TestVolatileEvictionMakesRoom(t *testing.T) {
	e, cs := newTestEngine(t)
	run(t, e, cs, "CONFIG", "SET", "maxmemory-policy", "volatile-lru")
	// The TTL'd key exists BEFORE the limit bites, so the gate's
	// eviction attempt has a victim when the over-limit write arrives.
	expectSimple(t, run(t, e, cs, "SET", "vol", "v", "EX", "100"), "OK")
	overLimit(e)
	expectSimple(t, run(t, e, cs, "SET", "a", "b"), "OK")
	if got := e.Shards.EvictedKeys.Load(); got < 1 {
		t.Error("volatile-lru did not evict the TTL'd key to make room")
	}
	if v := run(t, e, cs, "EXISTS", "vol"); v.Kind != resp.KindInt || v.Int != 0 {
		t.Errorf("vol should have been evicted, EXISTS = %+v", v)
	}
}

func TestConfigMaxmemoryPolicy(t *testing.T) {
	e, cs := newTestEngine(t)
	get := func() string {
		v := run(t, e, cs, "CONFIG", "GET", "maxmemory-policy")
		if v.Kind != resp.KindMap || len(v.Arr) != 2 {
			t.Fatalf("CONFIG GET maxmemory-policy = %+v", v)
		}
		return string(v.Arr[1].Blob)
	}
	if got := get(); got != "noeviction" {
		t.Fatalf("default maxmemory-policy = %q, want noeviction", got)
	}
	expectErr(t, run(t, e, cs, "CONFIG", "SET", "maxmemory-policy", "bogus"),
		"ERR CONFIG SET failed (possibly related to argument 'maxmemory-policy') - "+
			"argument(s) must be one of the following: "+strings.Join(shard.EvictPolicyNames, ", "))
	if got := get(); got != "noeviction" {
		t.Fatalf("maxmemory-policy after failed SET = %q, want noeviction", got)
	}
	// Case-insensitive on SET, canonical lowercase on GET (7.2.7 probe).
	expectSimple(t, run(t, e, cs, "CONFIG", "SET", "maxmemory-policy", "ALLKEYS-LFU"), "OK")
	if got := get(); got != "allkeys-lfu" {
		t.Fatalf("maxmemory-policy after SET ALLKEYS-LFU = %q, want allkeys-lfu", got)
	}
	for _, name := range shard.EvictPolicyNames {
		expectSimple(t, run(t, e, cs, "CONFIG", "SET", "maxmemory-policy", name), "OK")
		if got := get(); got != name {
			t.Fatalf("maxmemory-policy after SET %s = %q", name, got)
		}
	}
	expectSimple(t, run(t, e, cs, "CONFIG", "SET", "maxmemory-policy", "noeviction"), "OK")
}

func TestInfoMemoryFields(t *testing.T) {
	e, cs := newTestEngine(t)
	expectSimple(t, run(t, e, cs, "SET", "k", "v"), "OK")
	run(t, e, cs, "CONFIG", "SET", "maxmemory-policy", "allkeys-random")
	v := run(t, e, cs, "INFO", "memory")
	if v.Kind != resp.KindBlobString {
		t.Fatalf("INFO memory = %+v", v)
	}
	body := string(v.Blob)
	for _, want := range []string{"used_memory:", "used_memory_process:", "maxmemory:0", "maxmemory_policy:allkeys-random"} {
		if !strings.Contains(body, want) {
			t.Errorf("INFO memory missing %q:\n%s", want, body)
		}
	}
	v = run(t, e, cs, "INFO", "stats")
	if !strings.Contains(string(v.Blob), "evicted_keys:0") {
		t.Errorf("INFO stats missing evicted_keys:\n%s", v.Blob)
	}
}
