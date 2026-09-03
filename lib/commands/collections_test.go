package commands

import (
	"testing"

	"github.com/pschlump/ultima/lib/resp"
)

func TestParseFloat(t *testing.T) {
	good := map[string]float64{
		"1": 1, "+1": 1, "01": 1, ".5": 0.5, "1.": 1, "1e3": 1000,
		"0x10": 16, "0x1.8p1": 3, "2.5e1": 25, "1e-320": 1e-320,
	}
	for s, want := range good {
		if v, ok := parseFloat([]byte(s)); !ok || v != want {
			t.Errorf("parseFloat(%q) = %v,%v want %v,true", s, v, ok, want)
		}
	}
	for _, s := range []string{"", "abc", "1.5.2", "nan", "1_0", " 1", "1 ",
		"1e9999", "1e-9999", "1e", "0x"} {
		if v, ok := parseFloat([]byte(s)); ok {
			t.Errorf("parseFloat(%q) = %v,true want failure", s, v)
		}
	}
	for _, s := range []string{"inf", "-infinity", "+INF"} {
		v, ok := parseFloat([]byte(s))
		if !ok || !isInfF(v) {
			t.Errorf("parseFloat(%q) = %v,%v", s, v, ok)
		}
	}
}

func isInfF(f float64) bool {
	return f > 1e308 || f < -1e308
}

func TestFormatHumanFloat(t *testing.T) {
	cases := map[float64]string{
		5:      "5",
		1.1:    "1.10000000000000009",
		0.3:    "0.29999999999999999",
		300000: "300000",
		1e-20:  "0",
	}
	for f, want := range cases {
		if got := formatHumanFloat(f); got != want {
			t.Errorf("formatHumanFloat(%v) = %q, want %q", f, got, want)
		}
	}
}

func TestCollectionWrongTypeAndTypeCmd(t *testing.T) {
	e, cs := newTestEngine(t)
	run(t, e, cs, "HSET", "h", "f", "v")
	run(t, e, cs, "LPUSH", "l", "a")
	run(t, e, cs, "SADD", "s", "1")
	run(t, e, cs, "ZADD", "z", "1", "m")
	for k, want := range map[string]string{"h": "hash", "l": "list", "s": "set", "z": "zset"} {
		if v := run(t, e, cs, "TYPE", k); v.Kind != resp.KindSimpleString || v.Str != want {
			t.Errorf("TYPE %s = %+v, want %s", k, v, want)
		}
	}
	for _, args := range [][]string{
		{"GET", "h"}, {"GETDEL", "l"}, {"GETSET", "s", "x"}, {"GETEX", "z"},
		{"INCR", "h"}, {"APPEND", "l", "x"}, {"STRLEN", "s"},
		{"HGETALL", "l"}, {"LPUSH", "s", "x"}, {"SADD", "z", "x"}, {"ZADD", "h", "1", "m"},
	} {
		if v := run(t, e, cs, args...); v.Kind != resp.KindError ||
			v.Str != "WRONGTYPE Operation against a key holding the wrong kind of value" {
			t.Errorf("%v = %+v, want WRONGTYPE", args, v)
		}
	}
	// MGET silently nulls non-string keys.
	if v := run(t, e, cs, "MGET", "h", "missing"); v.Kind != resp.KindArray ||
		len(v.Arr) != 2 || v.Arr[0].Kind != resp.KindNull || v.Arr[1].Kind != resp.KindNull {
		t.Errorf("MGET = %+v", v)
	}
	// SET overwrites a collection key.
	if v := run(t, e, cs, "SET", "h", "plain"); v.Str != "OK" {
		t.Fatalf("SET over hash = %+v", v)
	}
	if v := run(t, e, cs, "TYPE", "h"); v.Str != "string" {
		t.Fatalf("TYPE after SET = %+v", v)
	}
}

func TestCollectionExpiryAndEmpties(t *testing.T) {
	e, cs := newTestEngine(t)
	// Empty collections must not exist as keys.
	run(t, e, cs, "HSET", "h", "f", "v")
	run(t, e, cs, "HDEL", "h", "f")
	run(t, e, cs, "LPUSH", "l", "a")
	run(t, e, cs, "LPOP", "l")
	run(t, e, cs, "SADD", "s", "1")
	run(t, e, cs, "SREM", "s", "1")
	run(t, e, cs, "ZADD", "z", "1", "m")
	run(t, e, cs, "ZREM", "z", "m")
	if v := run(t, e, cs, "DBSIZE"); v.Int != 0 {
		t.Fatalf("DBSIZE = %d, want 0", v.Int)
	}
	// Key-level TTL on a collection.
	run(t, e, cs, "HSET", "hx", "f", "v")
	run(t, e, cs, "PEXPIRE", "hx", "60000")
	if v := run(t, e, cs, "PTTL", "hx"); v.Int <= 0 {
		t.Fatalf("PTTL = %+v", v)
	}
	run(t, e, cs, "HSET", "hx", "g", "w") // in-place mutation keeps TTL
	if v := run(t, e, cs, "PTTL", "hx"); v.Int <= 0 {
		t.Fatalf("PTTL after mutation = %+v", v)
	}
}
