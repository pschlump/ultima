package scripting

// Conversion unit tests (M8): every golden value is probed against live
// Redis 7.2.7 — see docs/Redis-Errors.md §4/§5 for the wire-level forms.

import (
	"math"
	"testing"

	"github.com/yuin/gopher-lua/host"
	"github.com/pschlump/ultima/lib/resp"
)

func TestNumToInt(t *testing.T) {
	cases := []struct {
		in   float64
		want int64
	}{
		{1, 1}, {1.5, 1}, {-1.5, -1}, {0.1, 0}, {math.Copysign(0, -1), 0},
		{1e30, math.MaxInt64}, {math.Inf(1), math.MaxInt64},
		{math.Inf(-1), math.MinInt64}, {math.NaN(), 0},
		{9007199254740992, 9007199254740992}, // 2^53
		{9223372036854775807, math.MaxInt64},
		{9223372036854775808, math.MaxInt64},  // 2^63
		{-9223372036854775808, math.MinInt64}, // -2^63
		{1e15, 1000000000000000},
	}
	for _, c := range cases {
		if got := NumToInt(c.in); got != c.want {
			t.Errorf("NumToInt(%v) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestNumToArg(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		{1.5, "1.5"}, {1e30, "1e+30"}, {0.1, "0.1"}, {-7, "-7"},
		{9007199254740992, "9007199254740992"},
		{1.0 / 3.0, "0.3333333333333333"},
		{0.010000000000000002, "0.010000000000000002"}, // 0.1^2
		{math.Copysign(0, -1), "0"}, {1e15, "1000000000000000"},
		{123456789012345678, "123456789012345680"}, // f64 rounding
		{42, "42"}, {2, "2"},
	}
	for _, c := range cases {
		if got := NumToArg(c.in); got != c.want {
			t.Errorf("NumToArg(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestLuaToRESP(t *testing.T) {
	cases := []struct {
		name string
		in   host.Value
		want resp.Value
	}{
		{"nil", host.Nil(), resp.Null()},
		{"false", host.Bool(false), resp.Null()},
		{"true", host.Bool(true), resp.Int(1)},
		{"function", host.Value{Kind: host.KindFunction}, resp.Null()},
		{"int", host.Number(42), resp.Int(42)},
		{"float truncated", host.Number(1.5), resp.Int(1)},
		{"string", host.String("x"), resp.BlobStr("x")},
		{"empty table", host.Table(), resp.Arr()},
		{"hole truncates", host.Table(
			kv("unused", host.Nil()),
			host.KV{Key: host.Int(1), Val: host.Int(1)},
			host.KV{Key: host.Int(3), Val: host.Int(3)},
		), resp.Arr(resp.Int(1))},
		{"non-sequential ignored", host.Table(
			host.KV{Key: host.String("a"), Val: host.Int(1)},
		), resp.Arr()},
		{"err table", errTable("bad thing"), resp.Err("bad thing")},
		{"ok table", okTable("fine"), resp.Simple("fine")},
		{"err beats ok", host.Table(
			kv("ok", host.String("a")), kv("err", host.String("b")),
		), resp.Err("b")},
		{"non-string err ignored", host.Table(kv("err", host.Int(42))), resp.Arr()},
		{"map table", host.Table(kv("map", host.Table(
			host.KV{Key: host.String("a"), Val: host.Int(1)},
		))), resp.Map(resp.BlobStr("a"), resp.Int(1))},
		{"set table keys only", host.Table(kv("set", host.Table(
			host.KV{Key: host.String("m1"), Val: host.Bool(false)},
		))), resp.Set(resp.BlobStr("m1"))},
		{"double table", host.Table(kv("double", host.Number(2.5))), resp.Double(2.5)},
		{"map beats set", host.Table(
			kv("set", host.Table(host.KV{Key: host.String("s"), Val: host.Bool(true)})),
			kv("map", host.Table(host.KV{Key: host.String("a"), Val: host.Int(1)})),
		), resp.Map(resp.BlobStr("a"), resp.Int(1))},
		{"nested", host.Table(
			host.KV{Key: host.Int(1), Val: host.Int(1)},
			host.KV{Key: host.Int(2), Val: host.Int(2)},
			host.KV{Key: host.Int(3), Val: host.Table(
				host.KV{Key: host.Int(1), Val: host.Int(3)},
				host.KV{Key: host.Int(2), Val: host.String("four")},
			)},
		), resp.Arr(resp.Int(1), resp.Int(2), resp.Arr(resp.Int(3), resp.BlobStr("four")))},
		{"false in array", host.Table(
			host.KV{Key: host.Int(1), Val: host.Bool(false)},
			host.KV{Key: host.Int(2), Val: host.Bool(true)},
		), resp.Arr(resp.Null(), resp.Int(1))},
	}
	for _, c := range cases {
		if got := LuaToRESP(c.in, 2); !respEqual(got, c.want) {
			t.Errorf("%s: LuaToRESP = %+v, want %+v", c.name, got, c.want)
		}
	}
}

func respEqual(a, b resp.Value) bool {
	if a.Kind != b.Kind || a.Str != b.Str || a.Int != b.Int || a.Dbl != b.Dbl ||
		a.Bool != b.Bool || string(a.Blob) != string(b.Blob) || len(a.Arr) != len(b.Arr) {
		return false
	}
	for i := range a.Arr {
		if !respEqual(a.Arr[i], b.Arr[i]) {
			return false
		}
	}
	return true
}

func TestErrorReply(t *testing.T) {
	cases := []struct {
		name string
		in   *host.ScriptError
		want string
	}{
		{"string with position", &host.ScriptError{ErrValue: host.String("user_script:1: boom"), Line: 1},
			"ERR user_script:1: boom script: SHA, on @user_script:1."},
		{"level-0 string", &host.ScriptError{ErrValue: host.String("positional"), Line: 3},
			"ERR positional script: SHA, on @user_script:3."},
		{"number", &host.ScriptError{ErrValue: host.Number(42), Line: 1},
			"ERR user_script:1: 42 script: SHA, on @user_script:1."},
		{"bool", &host.ScriptError{ErrValue: host.Bool(true), Line: 1},
			"ERR true script: SHA, on @user_script:1."},
		{"nil", &host.ScriptError{ErrValue: host.Nil(), Line: 1},
			"ERR nil script: SHA, on @user_script:1."},
		{"err table verbatim", &host.ScriptError{ErrValue: errTable("x"), Line: 1},
			"x script: SHA, on @user_script:1."},
		{"bridge error verbatim", &host.ScriptError{ErrValue: errTable("ERR Wrong number of args calling Redis command from script"), Line: 3},
			"ERR Wrong number of args calling Redis command from script script: SHA, on @user_script:3."},
		{"kill text table-class", &host.ScriptError{ErrValue: errTable("ERR Script killed by user with SCRIPT KILL..."), Line: 1},
			"ERR Script killed by user with SCRIPT KILL... script: SHA, on @user_script:1."},
	}
	for _, c := range cases {
		got := ErrorReply("SHA", c.in)
		if got.Kind != resp.KindError || got.Str != c.want {
			t.Errorf("%s: ErrorReply = %v (%q), want error %q", c.name, got.Kind, got.Str, c.want)
		}
	}
}

func TestRespToLua(t *testing.T) {
	cases := []struct {
		name string
		in   resp.Value
		ver  int
		want host.Value
	}{
		{"null v2", resp.Null(), 2, host.Bool(false)},
		{"null v3", resp.Null(), 3, host.Nil()},
		{"int", resp.Int(7), 2, host.Number(7)},
		{"simple", resp.Simple("OK"), 2, okTable("OK")},
		{"blob", resp.BlobStr("v"), 2, host.String("v")},
		{"double v2 as string", resp.Double(1.5), 2, host.String("1.5")},
		{"double v3 wrapper", resp.Double(1.5), 3,
			host.Table(kv("double", host.Number(1.5)))},
		{"array", resp.Arr(resp.BlobStr("a"), resp.Int(2)), 2,
			host.Table(
				host.KV{Key: host.Int(1), Val: host.String("a")},
				host.KV{Key: host.Int(2), Val: host.Number(2)},
			)},
		{"map v2 flattened", resp.Map(resp.BlobStr("f"), resp.BlobStr("v")), 2,
			host.Table(
				host.KV{Key: host.Int(1), Val: host.String("f")},
				host.KV{Key: host.Int(2), Val: host.String("v")},
			)},
		{"map v3 wrapper", resp.Map(resp.BlobStr("f"), resp.BlobStr("v")), 3,
			host.Table(kv("map", host.Table(
				host.KV{Key: host.String("f"), Val: host.String("v")})))},
		{"set v3 wrapper", resp.Set(resp.BlobStr("m")), 3,
			host.Table(kv("set", host.Table(
				host.KV{Key: host.String("m"), Val: host.Bool(true)})))},
	}
	for _, c := range cases {
		if got := respToLua(c.in, c.ver); !hostEqual(got, c.want) {
			t.Errorf("%s: respToLua = %+v, want %+v", c.name, got, c.want)
		}
	}
}

func hostEqual(a, b host.Value) bool {
	if a.Kind != b.Kind || a.Num != b.Num || a.Str != b.Str || len(a.Pairs) != len(b.Pairs) {
		return false
	}
	for i := range a.Pairs {
		if !hostEqual(a.Pairs[i].Key, b.Pairs[i].Key) || !hostEqual(a.Pairs[i].Val, b.Pairs[i].Val) {
			return false
		}
	}
	return true
}
