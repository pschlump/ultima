package commands

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
	"go.uber.org/goleak"
)

// Guard against goroutine leaks: parked blocking waiters, broker state
// and push machinery are exactly what a regression here would leak (M3).
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

//go:embed manifest.json
var manifestJSON []byte

type manifestFile struct {
	Commands []struct {
		Name   string `json:"name"`
		Group  string `json:"group"`
		Status string `json:"status"`
		Since  string `json:"since"`
	} `json:"commands"`
}

func TestManifestMatchesTable(t *testing.T) {
	var mf manifestFile
	if err := json.Unmarshal(manifestJSON, &mf); err != nil {
		t.Fatal(err)
	}
	inManifest := map[string]string{}
	for _, c := range mf.Commands {
		if c.Status == "implemented" || c.Status == "partial" {
			inManifest[c.Name] = c.Group
		}
	}
	for name, def := range table {
		g, ok := inManifest[name]
		if !ok {
			t.Errorf("command %q in table but missing from manifest.json", name)
			continue
		}
		if g != def.Group {
			t.Errorf("command %q: manifest group %q != table group %q", name, g, def.Group)
		}
	}
	for name := range inManifest {
		if _, ok := table[name]; !ok {
			t.Errorf("manifest.json lists %q as implemented but it is not in the table", name)
		}
	}
}

func newTestEngine(t *testing.T) (*Engine, *ConnState) {
	t.Helper()
	sh := shard.NewEngine(4, 16)
	t.Cleanup(sh.Close)
	e := NewEngine(sh, "test", 6379)
	return e, e.NewConnState("127.0.0.1:1")
}

func run(t *testing.T, e *Engine, cs *ConnState, args ...string) resp.Value {
	t.Helper()
	bb := make([][]byte, len(args))
	for i, a := range args {
		bb[i] = []byte(a)
	}
	return e.Execute(cs, bb)
}

func TestParseIntStrict(t *testing.T) {
	good := map[string]int64{"0": 0, "1": 1, "-1": -1, "9223372036854775807": math.MaxInt64,
		"-9223372036854775808": math.MinInt64, "123": 123}
	for s, want := range good {
		if v, ok := parseIntStrict([]byte(s)); !ok || v != want {
			t.Errorf("parseIntStrict(%q) = %d,%v want %d,true", s, v, ok, want)
		}
	}
	for _, s := range []string{"", "-", "+1", " 1", "1 ", "01", "-0", "1.5", "abc",
		"9223372036854775808", "-9223372036854775809"} {
		if v, ok := parseIntStrict([]byte(s)); ok {
			t.Errorf("parseIntStrict(%q) = %d,true want failure", s, v)
		}
	}
}

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pat, s string
		want   bool
	}{
		{"*", "", true},
		{"*", "abc", true},
		{"a*", "abc", true},
		{"a*", "b", false},
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		{"h[ae]llo", "hello", true},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hello", false},
		{"h[^e]llo", "hallo", true},
		{"[a-c]x", "bx", true},
		{"[a-c]x", "dx", false},
		{"a\\*b", "a*b", true},
		{"a\\*b", "axb", false},
		{"", "", true},
		{"", "a", false},
	}
	for _, tc := range cases {
		if got := GlobMatch([]byte(tc.pat), []byte(tc.s)); got != tc.want {
			t.Errorf("GlobMatch(%q,%q) = %v, want %v", tc.pat, tc.s, got, tc.want)
		}
	}
}

func TestSetGetExpirySemantics(t *testing.T) {
	e, cs := newTestEngine(t)
	if v := run(t, e, cs, "SET", "a", "1"); v.Kind != resp.KindSimpleString || v.Str != "OK" {
		t.Fatalf("SET = %+v", v)
	}
	if v := run(t, e, cs, "GET", "a"); v.Kind != resp.KindBlobString || string(v.Blob) != "1" {
		t.Fatalf("GET = %+v", v)
	}
	if v := run(t, e, cs, "SET", "a", "2", "NX"); v.Kind != resp.KindNull {
		t.Fatalf("SET NX existing = %+v", v)
	}
	if v := run(t, e, cs, "GET", "a"); string(v.Blob) != "1" {
		t.Fatalf("GET after NX = %+v", v)
	}
	if v := run(t, e, cs, "SET", "a", "2", "XX", "GET"); v.Kind != resp.KindBlobString || string(v.Blob) != "1" {
		t.Fatalf("SET XX GET = %+v", v)
	}
	if v := run(t, e, cs, "GET", "a"); string(v.Blob) != "2" {
		t.Fatalf("GET after XX = %+v", v)
	}
	// past PXAT deletes
	if v := run(t, e, cs, "SET", "b", "1", "PXAT", "1"); v.Kind != resp.KindSimpleString || v.Str != "OK" {
		t.Fatalf("SET PXAT past = %+v", v)
	}
	if v := run(t, e, cs, "EXISTS", "b"); v.Int != 0 {
		t.Fatalf("EXISTS after past PXAT = %+v", v)
	}
	// KEEPTTL keeps the TTL
	run(t, e, cs, "SET", "c", "1", "PX", "60000")
	run(t, e, cs, "SET", "c", "2", "KEEPTTL")
	if v := run(t, e, cs, "PTTL", "c"); v.Kind != resp.KindInt || v.Int <= 0 {
		t.Fatalf("PTTL after KEEPTTL = %+v", v)
	}
	// plain SET clears TTL
	run(t, e, cs, "SET", "c", "3")
	if v := run(t, e, cs, "PTTL", "c"); v.Int != -1 {
		t.Fatalf("PTTL after plain SET = %+v", v)
	}
}

func TestIncrOverflow(t *testing.T) {
	e, cs := newTestEngine(t)
	run(t, e, cs, "SET", "n", "9223372036854775807")
	if v := run(t, e, cs, "INCR", "n"); v.Str != "ERR increment or decrement would overflow" {
		t.Fatalf("INCR overflow = %+v", v)
	}
	if v := run(t, e, cs, "DECRBY", "n", "-9223372036854775808"); v.Str != "ERR decrement would overflow" {
		t.Fatalf("DECRBY minint = %+v", v)
	}
	if v := run(t, e, cs, "INCR", "fresh"); v.Int != 1 {
		t.Fatalf("INCR fresh = %+v", v)
	}
}

func TestSelectAndNoAuth(t *testing.T) {
	e, cs := newTestEngine(t)
	e.SetRequirePass("pw")
	if v := run(t, e, cs, "GET", "x"); v.Str != "NOAUTH Authentication required." {
		t.Fatalf("GET unauthenticated = %+v", v)
	}
	if v := run(t, e, cs, "AUTH", "wrong"); v.Str != "WRONGPASS invalid username-password pair or user is disabled." {
		t.Fatalf("AUTH wrong = %+v", v)
	}
	if v := run(t, e, cs, "AUTH", "pw"); v.Kind != resp.KindSimpleString || v.Str != "OK" {
		t.Fatalf("AUTH = %+v", v)
	}
	if v := run(t, e, cs, "SELECT", "3"); v.Kind != resp.KindSimpleString || v.Str != "OK" {
		t.Fatalf("SELECT = %+v", v)
	}
	if v := run(t, e, cs, "SELECT", "16"); v.Str != "ERR DB index is out of range" {
		t.Fatalf("SELECT 16 = %+v", v)
	}
	run(t, e, cs, "SELECT", "3")
	run(t, e, cs, "SET", "k", "v3")
	run(t, e, cs, "SELECT", "0")
	if v := run(t, e, cs, "GET", "k"); v.Kind != resp.KindNull {
		t.Fatalf("GET k in db0 = %+v", v)
	}
	run(t, e, cs, "FLUSHALL")
	run(t, e, cs, "SELECT", "3")
	if v := run(t, e, cs, "GET", "k"); v.Kind != resp.KindNull {
		t.Fatalf("GET k in db3 after FLUSHALL = %+v", v)
	}
}

// TestMultiKeyFanOutConcurrent exercises the doMulti fan-out (DEL,
// EXISTS, MGET, MSETNX, SINTER, ZINTERSTORE) with keys spread across
// shards so the per-shard closures really run concurrently, plus
// duplicate keys for slot aliasing. Asserts Redis duplicate-key
// semantics; run with -race to catch unsynchronized shared writes.
func TestMultiKeyFanOutConcurrent(t *testing.T) {
	e, cs := newTestEngine(t)

	// One key per shard, so the fan-out is genuinely concurrent.
	byShard := map[int]string{}
	for i := 0; len(byShard) < 4 && i < 10000; i++ {
		k := fmt.Sprintf("fk:%d", i)
		if si := e.Shards.ShardIndex([]byte(k)); byShard[si] == "" {
			byShard[si] = k
		}
	}
	keys := make([]string, 0, len(byShard))
	for _, k := range byShard {
		keys = append(keys, k)
	}
	if len(keys) < 2 {
		t.Fatalf("need keys on >=2 shards, got %d", len(keys))
	}

	for range 100 {
		mset := []string{"MSET"}
		for _, k := range keys {
			mset = append(mset, k, "v:"+k)
		}
		run(t, e, cs, mset...)

		// MGET with a duplicate key repeats the value, as in Redis.
		v := run(t, e, cs, "MGET", keys[0], keys[1], keys[0])
		if v.Kind != resp.KindArray || len(v.Arr) != 3 {
			t.Fatalf("MGET = %+v", v)
		}
		for j, want := range []string{"v:" + keys[0], "v:" + keys[1], "v:" + keys[0]} {
			if string(v.Arr[j].Blob) != want {
				t.Fatalf("MGET[%d] = %q, want %q", j, v.Arr[j].Blob, want)
			}
		}

		// EXISTS counts duplicates (Redis >= 3.0.3).
		if v := run(t, e, cs, "EXISTS", keys[0], keys[1], keys[0]); v.Int != 3 {
			t.Fatalf("EXISTS = %+v, want 3", v)
		}

		// MSETNX across shards fails as a whole when any key exists.
		if v := run(t, e, cs, "MSETNX", keys[0], "x", "fk:new", "y"); v.Int != 0 {
			t.Fatalf("MSETNX existing = %+v, want 0", v)
		}
		if v := run(t, e, cs, "EXISTS", "fk:new"); v.Int != 0 {
			t.Fatalf("MSETNX wrote despite existing key: %+v", v)
		}

		// Cross-shard WRONGTYPE: both closures hit the error path
		// concurrently (was a shared errV write).
		if v := run(t, e, cs, "SINTER", keys[0], keys[1]); v.Kind != resp.KindError {
			t.Fatalf("SINTER on strings = %+v, want WRONGTYPE", v)
		}
		if v := run(t, e, cs, "ZINTERSTORE", "fk:zdst", "2", keys[0], keys[1]); v.Kind != resp.KindError {
			t.Fatalf("ZINTERSTORE on strings = %+v, want WRONGTYPE", v)
		}

		// DEL with a duplicate key deletes it once.
		if v := run(t, e, cs, "DEL", keys[0], keys[1], keys[0]); v.Int != 2 {
			t.Fatalf("DEL = %+v, want 2", v)
		}
	}
}
