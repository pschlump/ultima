package commands

// M8 Lua scripting engine tests (EVAL/EVALSHA/EVAL_RO/EVALSHA_RO/SCRIPT):
// semantics probed byte-exact against Redis 7.2.7 (docs/Redis-Errors.md).

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pschlump/ultima/lib/resp"
	"github.com/pschlump/ultima/lib/scripting"
	"github.com/pschlump/ultima/lib/shard"
)

func newScriptEngine(t *testing.T) (*Engine, *ConnState) {
	t.Helper()
	sh := shard.NewEngine(4, 16)
	t.Cleanup(sh.Close)
	e := NewEngine(sh, "test", 6379)
	m, err := scripting.New(scripting.Config{
		LuaTimeLimitMs: 5000, HardDeadlineMs: 2000, MaxMemoryMB: 16,
		CompatVersion: CompatVersion, RunID: e.RunID,
		VMPoolSize: 1, VMPoolMax: 16, VMRecycleRuns: 100, VMRecyclePct: 75,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	e.Scripts = m
	return e, e.NewConnState("127.0.0.1:1")
}

func evalArgs(script string, numkeys string, rest ...string) [][]byte {
	out := [][]byte{[]byte("EVAL"), []byte(script), []byte(numkeys)}
	for _, r := range rest {
		out = append(out, []byte(r))
	}
	return out
}

func wantErr(t *testing.T, v resp.Value, want string) {
	t.Helper()
	if v.Kind != resp.KindError || v.Str != want {
		t.Errorf("reply = %v (%q), want error %q", v.Kind, v.Str, want)
	}
}

func wantErrPrefix(t *testing.T, v resp.Value, prefix string) {
	t.Helper()
	if v.Kind != resp.KindError || !strings.HasPrefix(v.Str, prefix) {
		t.Errorf("reply = %v (%q), want error with prefix %q", v.Kind, v.Str, prefix)
	}
}

func TestEvalBasics(t *testing.T) {
	e, cs := newScriptEngine(t)
	if v := e.Execute(cs, evalArgs("return 1+1", "0")); v.Kind != resp.KindInt || v.Int != 2 {
		t.Errorf("EVAL 1+1 = %+v, want :2", v)
	}
	// KEYS/ARGV staging, binary safe.
	if v := e.Execute(cs, evalArgs("return {KEYS[1],ARGV[1]}", "1", "k1", "a\x00b")); v.Kind != resp.KindArray ||
		len(v.Arr) != 2 || string(v.Arr[0].Blob) != "k1" || string(v.Arr[1].Blob) != "a\x00b" {
		t.Errorf("KEYS/ARGV = %+v", v)
	}
	// Nested shapes.
	v := e.Execute(cs, evalArgs("return {1,2,{3,'four'}}", "0"))
	if v.Kind != resp.KindArray || len(v.Arr) != 3 || v.Arr[2].Kind != resp.KindArray ||
		string(v.Arr[2].Arr[1].Blob) != "four" {
		t.Errorf("nested = %+v", v)
	}
}

func TestEvalArgValidation(t *testing.T) {
	e, cs := newScriptEngine(t)
	wantErr(t, e.Execute(cs, evalArgs("return 1", "x")), "ERR value is not an integer or out of range")
	wantErr(t, e.Execute(cs, evalArgs("return 1", "-1")), "ERR Number of keys can't be negative")
	wantErr(t, e.Execute(cs, evalArgs("return 1", "2", "a")), "ERR Number of keys can't be greater than number of args")
	wantErr(t, e.Execute(cs, evalArgs("return 1", "1")), "ERR Number of keys can't be greater than number of args")
	wantErr(t, e.Execute(cs, [][]byte{[]byte("EVAL"), []byte("return 1")}), "ERR wrong number of arguments for 'eval' command")
	wantErr(t, e.Execute(cs, [][]byte{[]byte("EVALSHA"), []byte("deadbeef"), []byte("0")}), "NOSCRIPT No matching script. Please use EVAL.")
}

func TestEvalScriptErrors(t *testing.T) {
	e, cs := newScriptEngine(t)
	// Runtime string error: position prefix + script suffix with the sha.
	v := e.Execute(cs, evalArgs("return error('boom')", "0"))
	sha := "57b11f4c6516f4ef2661c6423451796106f8393a" // sha1("return error('boom')")
	wantErr(t, v, "ERR user_script:1: boom script: "+sha+", on @user_script:1.")
	// The raise line tracks multiline scripts.
	v = e.Execute(cs, evalArgs("local a = 1\nlocal b = 2\nerror('boom3')", "0"))
	wantErrPrefix(t, v, "ERR user_script:3: boom3 script: ")
	if !strings.HasSuffix(v.Str, ", on @user_script:3.") {
		t.Errorf("suffix = %q, want ... on @user_script:3.", v.Str)
	}
	// error() table with err field: verbatim.
	v = e.Execute(cs, evalArgs("return error({err='table boom'})", "0"))
	wantErrPrefix(t, v, "table boom script: ")
	// A table WITHOUT an err field crashes Redis 7.2.7; Ultima answers cleanly.
	if v := e.Execute(cs, evalArgs("return error({})", "0")); v.Kind != resp.KindError {
		t.Errorf("error({{}}) = %+v, want a clean error", v)
	}
	// Compile error shape (inner wording is the gopher-lua frontend's —
	// ledgered divergence, docs/Redis-Errors.md §11).
	wantErrPrefix(t, e.Execute(cs, evalArgs("this is not lua", "0")),
		"ERR Error compiling script (new function):")
}

func TestScriptCommand(t *testing.T) {
	e, cs := newScriptEngine(t)
	// LOAD → 40-hex sha; EXISTS; EVALSHA; FLUSH.
	v := e.Execute(cs, [][]byte{[]byte("SCRIPT"), []byte("LOAD"), []byte("return 42")})
	if v.Kind != resp.KindBlobString || len(v.Blob) != 40 {
		t.Fatalf("SCRIPT LOAD = %+v, want 40-hex bulk", v)
	}
	sha := string(v.Blob)
	if sha != "1fa00e76656cc152ad327c13fe365858fd7be306" {
		t.Errorf("sha = %q, want the Redis-identical 1fa00e76…", sha)
	}
	v = e.Execute(cs, [][]byte{[]byte("SCRIPT"), []byte("EXISTS"), []byte(sha), []byte("deadbeef")})
	if v.Kind != resp.KindArray || len(v.Arr) != 2 || v.Arr[0].Int != 1 || v.Arr[1].Int != 0 {
		t.Errorf("SCRIPT EXISTS = %+v", v)
	}
	if v := e.Execute(cs, [][]byte{[]byte("EVALSHA"), []byte(sha), []byte("0")}); v.Int != 42 {
		t.Errorf("EVALSHA = %+v, want :42", v)
	}
	if v := e.Execute(cs, [][]byte{[]byte("SCRIPT"), []byte("FLUSH"), []byte("ASYNC")}); v.Kind != resp.KindSimpleString {
		t.Errorf("SCRIPT FLUSH ASYNC = %+v", v)
	}
	v = e.Execute(cs, [][]byte{[]byte("SCRIPT"), []byte("EXISTS"), []byte(sha)})
	if v.Arr[0].Int != 0 {
		t.Errorf("EXISTS after FLUSH = %+v", v)
	}
	// Error forms (all probed).
	wantErr(t, e.Execute(cs, [][]byte{[]byte("SCRIPT")}), "ERR wrong number of arguments for 'script' command")
	wantErr(t, e.Execute(cs, [][]byte{[]byte("SCRIPT"), []byte("FOO")}), "ERR unknown subcommand 'FOO'. Try SCRIPT HELP.")
	wantErr(t, e.Execute(cs, [][]byte{[]byte("SCRIPT"), []byte("KILL")}), "NOTBUSY No scripts in execution right now.")
	wantErr(t, e.Execute(cs, [][]byte{[]byte("SCRIPT"), []byte("EXISTS")}), "ERR wrong number of arguments for 'script|exists' command")
	wantErr(t, e.Execute(cs, [][]byte{[]byte("SCRIPT"), []byte("LOAD")}), "ERR wrong number of arguments for 'script|load' command")
	wantErr(t, e.Execute(cs, [][]byte{[]byte("SCRIPT"), []byte("FLUSH"), []byte("BADMODE")}), "ERR SCRIPT FLUSH only support SYNC|ASYNC option")
}

func TestEvalRedisCall(t *testing.T) {
	e, cs := newScriptEngine(t)
	if v := e.Execute(cs, evalArgs("return redis.call('set',KEYS[1],ARGV[1])", "1", "k", "v")); v.Kind != resp.KindSimpleString || v.Str != "OK" {
		t.Fatalf("redis.call set = %+v", v)
	}
	if v := e.Execute(cs, [][]byte{[]byte("GET"), []byte("k")}); string(v.Blob) != "v" {
		t.Fatalf("GET k = %+v", v)
	}
	if v := e.Execute(cs, evalArgs("return redis.call('get','k')", "0")); string(v.Blob) != "v" {
		t.Errorf("redis.call get = %+v", v)
	}
	// null → Lua false.
	if v := e.Execute(cs, evalArgs("return redis.call('get','missing') == false", "0")); v.Int != 1 {
		t.Errorf("null→false = %+v", v)
	}
	// status reply → {ok} table.
	if v := e.Execute(cs, evalArgs("return redis.call('set','k','v').ok", "0")); string(v.Blob) != "OK" {
		t.Errorf(".ok = %+v", v)
	}
	// Blocking commands run non-blocking from scripts (probed).
	if v := e.Execute(cs, evalArgs("return redis.call('blpop','emptylist',0)", "0")); v.Kind != resp.KindNull {
		t.Errorf("blpop empty = %+v, want null", v)
	}
	e.Execute(cs, [][]byte{[]byte("RPUSH"), []byte("bl"), []byte("x")})
	if v := e.Execute(cs, evalArgs("return redis.call('blpop','bl',0)", "0")); v.Kind != resp.KindArray || len(v.Arr) != 2 {
		t.Errorf("blpop with data = %+v", v)
	}
}

func TestEvalRedisCallErrors(t *testing.T) {
	e, cs := newScriptEngine(t)
	suffix := func(script string) string {
		return fmt.Sprintf(" script: %s, on @user_script:1.", sha1hexOf(script))
	}
	cases := []struct{ script, text string }{
		{"return redis.call()", "ERR Please specify at least one argument for this redis lib call"},
		{"return redis.call('get')", "ERR Wrong number of args calling Redis command from script"},
		{"return redis.call(true,'x')", "ERR Lua redis lib command arguments must be strings or integers"},
		{"return redis.call('set','k',nil)", "ERR Lua redis lib command arguments must be strings or integers"},
		{"return redis.call('nosuchcmd','x')", "ERR Unknown Redis command called from script"},
		{"return redis.call('eval','return 1','0')", "ERR This Redis command is not allowed from script"},
		{"return redis.call('script','load','return 1')", "ERR This Redis command is not allowed from script"},
		{"return redis.call('subscribe','c')", "ERR This Redis command is not allowed from script"},
		{"return redis.call('multi')", "ERR This Redis command is not allowed from script"},
		{"return redis.call('watch','k')", "ERR This Redis command is not allowed from script"},
		{"return redis.call('config','get','maxmemory')", "ERR This Redis command is not allowed from script"},
		{"return redis.call('auth','x')", "ERR This Redis command is not allowed from script"},
		{"return redis.call('monitor')", "ERR This Redis command is not allowed from script"},
	}
	for _, c := range cases {
		wantErr(t, e.Execute(cs, evalArgs(c.script, "0")), c.text+suffix(c.script))
	}
	// The error line tracks the redis.call line.
	v := e.Execute(cs, evalArgs("local a = 1\nlocal b = 2\nreturn redis.call('get')", "0"))
	if !strings.HasSuffix(v.Str, ", on @user_script:3.") {
		t.Errorf("multiline redis.call error suffix = %q", v.Str)
	}
	// pcall: {err} table, no suffix when returned.
	wantErr(t, e.Execute(cs, evalArgs("return redis.pcall('get')", "0")),
		"ERR Wrong number of args calling Redis command from script")
	// pcall of a good command works.
	if v := e.Execute(cs, evalArgs("return redis.pcall('set','pk','1')", "0")); v.Kind != resp.KindSimpleString {
		t.Errorf("pcall set = %+v", v)
	}
	// error_reply / status_reply and their usage errors (no suffix).
	wantErr(t, e.Execute(cs, evalArgs("return redis.error_reply('my err')", "0")), "my err")
	if v := e.Execute(cs, evalArgs("return redis.status_reply('my status')", "0")); v.Kind != resp.KindSimpleString || v.Str != "my status" {
		t.Errorf("status_reply = %+v", v)
	}
	wantErr(t, e.Execute(cs, evalArgs("return redis.error_reply()", "0")), "ERR wrong number or type of arguments")
	// sha1hex.
	if v := e.Execute(cs, evalArgs("return redis.sha1hex('abc')", "0")); string(v.Blob) != "a9993e364706816aba3e25717850c26c9cd0d89d" {
		t.Errorf("sha1hex = %+v", v)
	}
	// Constants.
	if v := e.Execute(cs, evalArgs("return {redis.LOG_WARNING, redis.REDIS_VERSION}", "0")); v.Arr[0].Int != 3 || string(v.Arr[1].Blob) != CompatVersion {
		t.Errorf("constants = %+v", v)
	}
	// Allowed from scripts: flushall, publish, select, ping, keys.
	if v := e.Execute(cs, evalArgs("return redis.call('flushall')", "0")); v.Kind != resp.KindSimpleString {
		t.Errorf("flushall from script = %+v", v)
	}
	if v := e.Execute(cs, evalArgs("return redis.call('publish','chan','msg')", "0")); v.Int != 0 {
		t.Errorf("publish from script = %+v", v)
	}
}

func TestEvalReadOnly(t *testing.T) {
	e, cs := newScriptEngine(t)
	ro := [][]byte{[]byte("EVAL_RO"), []byte("return redis.call('set','rk','v')"), []byte("0")}
	wantErr(t, e.Execute(cs, ro),
		"ERR Write commands are not allowed from read-only scripts. script: 67372e76f490973b7669649bca9fad9c4d8ea17b, on @user_script:1.")
	if v := e.Execute(cs, [][]byte{[]byte("EVAL_RO"), []byte("return 1+1"), []byte("0")}); v.Int != 2 {
		t.Errorf("EVAL_RO 1+1 = %+v", v)
	}
	// Reads are fine, including reads of real data.
	e.Execute(cs, [][]byte{[]byte("SET"), []byte("rk"), []byte("v")})
	if v := e.Execute(cs, [][]byte{[]byte("EVAL_RO"), []byte("return redis.call('get','rk')"), []byte("0")}); string(v.Blob) != "v" {
		t.Errorf("EVAL_RO get = %+v", v)
	}
}

func TestEvalInsideMulti(t *testing.T) {
	e, cs := newScriptEngine(t)
	e.Execute(cs, [][]byte{[]byte("MULTI")})
	if v := e.Execute(cs, evalArgs("return redis.call('set','tx1','a')", "0")); v.Kind != resp.KindSimpleString || v.Str != "QUEUED" {
		t.Fatalf("EVAL in MULTI = %+v, want QUEUED", v)
	}
	e.Execute(cs, evalArgs("return 1", "0"))
	v := e.Execute(cs, [][]byte{[]byte("EXEC")})
	if v.Kind != resp.KindArray || len(v.Arr) != 2 || v.Arr[0].Str != "OK" || v.Arr[1].Int != 1 {
		t.Fatalf("EXEC = %+v, want [OK 1]", v)
	}
	if v := e.Execute(cs, [][]byte{[]byte("GET"), []byte("tx1")}); string(v.Blob) != "a" {
		t.Errorf("GET tx1 = %+v", v)
	}
}

// FLUSHALL/FLUSHDB/DBSIZE fan out shard tasks; queued in MULTI they run
// under EXEC's pause and must carry the token (they deadlocked before
// FlushDBTok/DBSizeTok existed).
func TestFlushFanoutInsideMulti(t *testing.T) {
	e, cs := newScriptEngine(t)
	e.Execute(cs, [][]byte{[]byte("SET"), []byte("k"), []byte("v")})
	e.Execute(cs, [][]byte{[]byte("MULTI")})
	e.Execute(cs, [][]byte{[]byte("FLUSHALL")})
	e.Execute(cs, [][]byte{[]byte("DBSIZE")})
	e.Execute(cs, [][]byte{[]byte("SET"), []byte("k2"), []byte("v2")})
	v := e.Execute(cs, [][]byte{[]byte("EXEC")})
	if v.Kind != resp.KindArray || len(v.Arr) != 3 || v.Arr[0].Str != "OK" || v.Arr[1].Int != 0 || v.Arr[2].Str != "OK" {
		t.Fatalf("EXEC = %+v, want [OK 0 OK]", v)
	}
	if v := e.Execute(cs, [][]byte{[]byte("GET"), []byte("k2")}); string(v.Blob) != "v2" {
		t.Errorf("GET k2 = %+v", v)
	}
}

func TestEvalMonitorForm(t *testing.T) {
	e, _ := newScriptEngine(t)
	var events []MonitorEvent
	cancel := e.AddMonitor(func(ev MonitorEvent) { events = append(events, ev) })
	defer cancel()
	mon := e.NewConnState("127.0.0.1:9")
	e.Execute(mon, evalArgs("redis.call('set','mk','mv') return redis.call('get','mk')", "1", "mk"))
	var luaEvents int
	var lastIsEval bool
	for _, ev := range events {
		if ev.Addr == "lua" {
			luaEvents++
			if string(ev.Args[0]) != "set" && string(ev.Args[0]) != "get" {
				t.Errorf("lua monitor args = %q", ev.Args[0])
			}
		} else if len(ev.Args) > 0 && lowerASCII(ev.Args[0]) == "eval" {
			lastIsEval = true
		}
	}
	if luaEvents != 2 {
		t.Errorf("lua monitor events = %d, want 2 (set + get)", luaEvents)
	}
	if !lastIsEval {
		t.Errorf("EVAL itself missing from the monitor feed")
	}
}

// sha1hexOf mirrors the guest identity for suffix assertions.
func sha1hexOf(script string) string {
	m, err := scripting.New(scripting.Config{CompatVersion: CompatVersion})
	if err != nil {
		panic(err)
	}
	defer func() { _ = m.Close() }()
	sha, _, err := m.Compile([]byte(script))
	if err != nil {
		panic(err)
	}
	return sha
}
