package commands

import (
	_ "embed"
	"encoding/json"
	"math"
	"testing"

	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/shard"
)

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
