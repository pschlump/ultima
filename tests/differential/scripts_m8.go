package differential

// M8 scripting corpus (EVAL/EVALSHA/EVAL_RO/EVALSHA_RO/SCRIPT): every row
// is byte-exact against Redis 7.2.7 (probes: docs/Redis-Errors.md).
//
// Deliberately absent (divergences or untestable — documented in
// docs/Redis-Errors.md §11):
//   - Compile-error WORDING (gopher-lua frontend ≠ PUC Lua): only the
//     "ERR Error compiling script (new function):" shape matches.
//   - Lua runtime-error texts (gopher dialect ≠ PUC: "attempt to call a
//     non-function object" vs Redis's "attempt to call a nil value" /
//     "Script attempted to access nonexistent global variable 'x'").
//   - error() of a table without an err field — CRASHES Redis 7.2.7
//     (verified); Ultima answers cleanly. Untestable here.
//   - SCRIPT DEBUG (7.2.7 answers OK; Ultima refuses — no debug hooks).
//   - math.random VALUES (different streams by design, S6; only the
//     shapes are asserted).
//   - UNKILLABLE/SHUTDOWN NOSAVE (killing the harness's redis-server
//     would take the suite down); covered by tests/m8_script_test.go.
//   - The hard-deadline watchdog (S5 divergence: Redis cannot preempt).
//   - CONFIG GET * wildcards: Ultima exposes extra script-* keys.
//   - tostring() of floats inside scripts (guest number dialect, ledger
//     rows 38/40/48 — reply formatting is host-side and unaffected).

var m8Scripts = func() []script {
	out := make([]script, 0, 16)
	out = append(out, m8ConversionScripts...)
	out = append(out, m8EvalCommandScripts...)
	out = append(out, m8CallScripts...)
	out = append(out, m8FlowScripts...)
	return out
}()

var m8ConversionScripts = []script{
	{"eval-number-conversions", []step{
		cmd("EVAL", "return 1", "0"),
		cmd("EVAL", "return 1.5", "0"),  // truncated toward zero
		cmd("EVAL", "return -1.5", "0"), // -1
		cmd("EVAL", "return 0.1", "0"),  // 0
		cmd("EVAL", "return -0", "0"),   // 0
		cmd("EVAL", "return 1e30", "0"), // clamped to int64 max
		cmd("EVAL", "return 2^53", "0"), // 9007199254740992
		cmd("EVAL", "return math.huge", "0"),
		cmd("EVAL", "return -math.huge", "0"),
		cmd("EVAL", "return 0/0", "0"), // NaN → 0
		cmd("EVAL", "return 1e15", "0"),
		cmd("EVAL", "return 12345678901234567890", "0"), // clamped
		cmd("EVAL", "return 9223372036854775807", "0"),
		cmd("EVAL", "return 9223372036854775808", "0"), // f64 rounds to 2^63 → max
	}},
	{"eval-scalar-conversions", []step{
		cmd("EVAL", "return 'x'", "0"),
		cmd("EVAL", "return 'tab\there'", "0"),
		cmd("EVAL", "return 'nul\\0byte'", "0"),
		cmd("EVAL", "return true", "0"),
		cmd("EVAL", "return false", "0"),
		cmd("EVAL", "return nil", "0"),
		cmd("EVAL", "return function() end", "0"),
		cmd("EVAL", "return 1,2,3", "0"), // only the first value
		cmd("EVAL", "return", "0"),
	}},
	{"eval-table-conversions", []step{
		cmd("EVAL", "return {}", "0"),        // empty array, not null
		cmd("EVAL", "return {1,nil,3}", "0"), // truncated at the hole
		cmd("EVAL", "return {1,2,{3,'four'}}", "0"),
		cmd("EVAL", "return {a=1}", "0"), // *0
		cmd("EVAL", "return {[0]='z',[1]='a'}", "0"),
		cmd("EVAL", "return {[-1]='n',[1]='a'}", "0"),
		cmd("EVAL", "return {[1.5]='f',[1]='a'}", "0"),
		cmd("EVAL", "return {[2]='b'}", "0"), // *0
		cmd("EVAL", "return {false,true}", "0"),
		cmd("EVAL", "return {false}", "0"),
		cmd("EVAL", "return {nil}", "0"),
		cmd("EVAL", "return {{}}", "0"),
		cmd("EVAL", "return {'a',{}}", "0"),
		cmd("EVAL", "return {1.5, 2}", "0"),
		cmd("EVAL", "return #{1,2,3}", "0"),
		cmd("EVAL", "return {err='bad thing'}", "0"),
		cmd("EVAL", "return {ok='fine'}", "0"),
		cmd("EVAL", "return {ok='a', err='b'}", "0"), // err beats ok
		cmd("EVAL", "return {err='x', 1}", "0"),
		cmd("EVAL", "return {err=42}", "0"),   // *0 (non-string ignored)
		cmd("EVAL", "return {ok=42}", "0"),    // *0
		cmd("EVAL", "return {err=true}", "0"), // *0
	}},
	{"eval-map-set-double-tables", []step{
		cmd("EVAL", "return {map={a=1}}", "0"),
		cmd("EVAL", "return {map={}}", "0"),
		cmd("EVAL", "return {map='str'}", "0"),
		cmd("EVAL", "return {map={a=1}, err='x'}", "0"),  // err beats map
		cmd("EVAL", "return {ok='y', map={a=1}}", "0"),   // ok beats map
		cmd("EVAL", "return {map={x={map={y=1}}}}", "0"), // nested maps
		cmd("EVAL", "return {set={m1=true, m2=true}}", "0"),
		cmd("EVAL", "return {set={m1=false}}", "0"), // values ignored
		cmd("EVAL", "return {set={}}", "0"),
		cmd("EVAL", "return {set='str'}", "0"),
		cmd("EVAL", "return {set={m1=true}, map={a=1}}", "0"), // map beats set
		cmd("EVAL", "return {double=2.5}", "0"),
		cmd("EVAL", "return {double=2}", "0"),
		cmd("EVAL", "return {double='x'}", "0"), // *0
		cmd("EVAL", "return {double=2.5, err='x'}", "0"),
		cmdM(mSetCmp, "EVAL", "return {map={a=1,b=2}}", "0"), // pair order undefined
		cmdM(mSetCmp, "EVAL", "return {set={m1=true,m2=true}}", "0"),
	}},
	{"eval-globals-guard", []step{
		// Redis 7.2.7 script environment lockdown (script_lua.c +
		// deps/lua readonly-table patch, ported to the gopher-lua
		// runtime in M8d): undefined-global reads raise via the _G
		// error metatable; globals and every reachable table are
		// readonly; KEYS/ARGV stay writable; rawget bypasses the guard.
		cmd("EVAL", "return undefined_global", "0"),
		cmd("EVAL", "g = 5 return g", "0"),
		cmd("EVAL", "return _G[nil]", "0"),
		cmd("EVAL", "rawset(_G, 'g3', 1) return 1", "0"),
		cmd("EVAL", "rawseti(_G, 1, 'x') return 1", "0"), // no such builtin: guard fires first
		cmd("EVAL", "return rawget(_G, 'undefined_global') == nil", "0"),
		cmd("EVAL", "string.foo = 1 return 1", "0"),
		cmd("EVAL", "redis.error_reply = nil return 1", "0"),
		cmd("EVAL", "redis = nil return 1", "0"),
		cmd("EVAL", "math.huge = 5 return math.huge", "0"),
		cmd("EVAL", "KEYS[1] = 'x' return KEYS[1]", "1", "k"),
		cmd("EVAL", "ARGV[1] = 'y' return ARGV[1]", "0", "a"),
		cmd("EVAL", "_G.pairs = nil return 1", "0"),
		cmd("EVAL", "local ok,e = pcall(function() return nosuchglobal end) return {ok, e}", "0"),
		cmd("EVAL", "local ok,e = pcall(function() g = 5 end) return {ok, e}", "0"),
		cmd("EVAL", "local t = setmetatable({}, {__index=function() return 7 end}) return t.x", "0"),
		cmd("EVAL", "return getmetatable(_G) ~= nil", "0"),
		cmd("EVAL", "for k in pairs(_G) do if k=='nosuch' then return 1 end end return 0", "0"),
		cmd("EVAL", "local t = {} rawset(t, 'k', 1) return t.k", "0"),
		cmd("EVAL", "return _G", "0"), // a table: empty array reply
	}},
	{"eval-args-and-globals", []step{
		cmd("EVAL", "return #KEYS", "0"),
		cmd("EVAL", "return #ARGV", "1", "k1", "a1", "a2"),
		cmd("EVAL", "return KEYS[1]", "0"), // nil → null
		cmd("EVAL", "return {KEYS[1],KEYS[2],ARGV[1]}", "2", "k1", "k2", "a1"),
		cmd("EVAL", "return #ARGV", "0", "extra1", "extra2"), // extras land in ARGV
		cmd("EVAL", "return redis.LOG_WARNING", "0"),
		cmd("EVAL", "return redis.REDIS_VERSION", "0"),
		cmd("EVAL", "return redis.sha1hex('abc')", "0"),
		cmd("EVAL", "return redis.sha1hex('')", "0"),
		cmd("EVAL", "return type(redis.call)", "0"),
		cmd("EVAL", "return type(redis.log)", "0"),
	}},
	{"eval-runtime-errors", []step{
		cmd("EVAL", "return error('boom')", "0"),
		cmd("EVAL", "local a = 1\nlocal b = 2\nerror('boom3')", "0"), // line 3
		cmd("EVAL", "return error('positional', 0)", "0"),
		cmd("EVAL", "local a = 1\nlocal b = 2\nerror('boom3', 0)", "0"),
		cmd("EVAL", "return error(42)", "0"),
		// error(42, 0) is excluded: the staged value carries no error()
		// LEVEL, so level-sensitive position rendering of number errors
		// is not tracked (ledgered, docs/Redis-Errors.md §11).
		cmd("EVAL", "return error(true)", "0"),
		cmd("EVAL", "return error(nil)", "0"),
		cmd("EVAL", "return error('WRONGTYPE foo')", "0"),
		cmd("EVAL", "return error('ERR already')", "0"),
		cmd("EVAL", "return error('')", "0"),
		cmd("EVAL", "return error({err='table boom'})", "0"),
		cmd("EVAL", "return error({err='x', code=5})", "0"),
		// error('ERR already', 0) is excluded: level-0 user strings
		// starting with an error-code word collide with the redis.call
		// error class (Redis's true distinguisher is a metatable).
		cmd("EVAL", "local r = redis.pcall('get') error(r) return 1", "0"), // re-raised {err}
		cmd("EVAL", "local ok, err = pcall(function() return redis.call('get') end) return {tostring(ok), err}", "0"),
		cmd("EVAL", "local ok, err = pcall(function() local x = 1\nlocal y = 2\nerror('inner') end) error(err)", "0"),
	}},
	{"eval-pcall-string-model", []step{
		// redis.call raises an error STRING (Redis adds a metatable we
		// cannot reproduce — invisible in these observable forms).
		cmd("EVAL", "local ok, err = pcall(function() return redis.call('get') end) return type(err)", "0"),
		cmd("EVAL", "local ok, err = pcall(function() return redis.call('get') end) return tostring(err)", "0"),
		cmd("EVAL", "local ok, err = pcall(function() return redis.call('get') end) return err.err", "0"),
		cmd("EVAL", "local ok, err = pcall(function() return redis.call('get') end) return {err}", "0"),
		cmd("EVAL", "local ok, err = pcall(function() return redis.call('get') end) return {err.err}", "0"),
		cmd("EVAL", "local ok, err = pcall(function() return redis.call('get') end) return err", "0"),
		// redis.pcall returns {err=..., ignore_error_stats_update=1}.
		cmd("EVAL", "local r = redis.pcall('get') return r.err", "0"),
		cmd("EVAL", "local r = redis.pcall('get') return r.ignore_error_stats_update", "0"),
		cmd("EVAL", "local r = redis.pcall('get') return {r}", "0"),
		cmd("EVAL", "local r = redis.pcall('get') return {r.err}", "0"),
		// Nested err/ok tables become error/simple elements at any depth.
		cmd("EVAL", "return {{err='x'}}", "0"),
		cmd("EVAL", "return {'y', {err='x'}}", "0"),
		cmd("EVAL", "return {{ok='y'}}", "0"),
		cmd("EVAL", "return {map={a={err='e'}}}", "0"),
		// error_reply: a bare word gets "ERR " prepended (probed).
		cmd("EVAL", "return redis.error_reply('x').err", "0"),
		cmd("EVAL", "return redis.error_reply('x y').err", "0"),
		cmd("EVAL", "return redis.error_reply('').err", "0"),
		cmd("EVAL", "return redis.error_reply('ERR').err", "0"),
		cmd("EVAL", "return redis.error_reply('my err')", "0"),
		cmd("EVAL", "return redis.error_reply('ERR coded')", "0"),
	}},
	{"eval-resp3-leg", []step{
		cmdM(mHello, "HELLO", "3"),
		cmd("EVAL", "return {1,2}", "0"),
		cmd("EVAL", "return {map={a=1}}", "0"),     // %1
		cmd("EVAL", "return {set={m1=true}}", "0"), // ~1
		cmd("EVAL", "return {double=2.5}", "0"),    // ,2.5
		cmd("EVAL", "return false", "0"),           // _
		cmd("EVAL", "return {false,true}", "0"),
		cmd("EVAL", "return {err='x'}", "0"),
		cmd("EVAL", "redis.setresp(3) return redis.call('hgetall','nope')", "0"),
		cmd("RECONNECT"),
	}},
}

var m8EvalCommandScripts = []script{
	{"eval-arg-validation", []step{
		cmd("EVAL", "return 1", "x"),
		cmd("EVAL", "return 1", "-1"),
		cmd("EVAL", "return 1", "2", "a"),
		cmd("EVAL", "return 1", "1"),
		cmd("EVAL", "return 1", "0x"),
		cmd("EVAL", "return 1"),
		cmd("EVAL"),
		cmd("EVALSHA", "deadbeef", "0"),
		cmd("EVALSHA", "xyz", "0"),
		cmd("EVALSHA", "1fa00e76656cc152ad327c13fe365858fd7be306"), // arity before NOSCRIPT
		cmd("EVALSHA", "1fa00e76656cc152ad327c13fe365858fd7be306", "x"),
	}},
	{"script-command", []step{
		cmd("SCRIPT", "LOAD", "return 42"),
		cmd("SCRIPT", "EXISTS", "1fa00e76656cc152ad327c13fe365858fd7be306", "deadbeef"),
		cmd("EVALSHA", "1fa00e76656cc152ad327c13fe365858fd7be306", "0"),
		// Same body via EVAL hits the same cache entry.
		cmd("SCRIPT", "EXISTS", "57b11f4c6516f4ef2661c6423451796106f8393a"),
		cmd("EVAL", "return error('boom')", "0"),
		cmd("SCRIPT", "EXISTS", "57b11f4c6516f4ef2661c6423451796106f8393a"),
		cmd("SCRIPT", "FLUSH", "ASYNC"),
		cmd("SCRIPT", "EXISTS", "1fa00e76656cc152ad327c13fe365858fd7be306"),
		cmd("SCRIPT", "FLUSH", "SYNC"),
		cmd("SCRIPT", "FLUSH"),
		cmd("SCRIPT", "LOAD", "return 42"),
		cmd("SCRIPT", "FLUSH"),
		// Errors.
		cmd("SCRIPT"),
		cmd("SCRIPT", "FOO"),
		cmd("SCRIPT", "foo"), // subcommand casing in the error
		cmd("SCRIPT", "KILL"),
		cmd("SCRIPT", "EXISTS"),
		cmd("SCRIPT", "LOAD"),
		cmd("SCRIPT", "LOAD", "a", "b"),
		cmd("SCRIPT", "FLUSH", "BADMODE"),
		cmd("SCRIPT", "FLUSH", "async", "extra"),
		cmd("SCRIPT", "HELP"),
	}},
	{"eval-ro", []step{
		cmd("SET", "rok", "rov"),
		cmd("EVAL_RO", "return redis.call('get','rok')", "0"),
		cmd("EVAL_RO", "return redis.call('set','rk','v')", "0"),
		cmd("EVAL_RO", "return redis.pcall('set','rk','v')", "0"), // {err} back
		cmd("GET", "rk"), // the pcall'd write must not have happened
		cmd("EVAL_RO", "return 1+1", "0"),
		cmd("EVALSHA_RO", "67372e76f490973b7669649bca9fad9c4d8ea17b", "0"), // not cached yet
		cmd("SCRIPT", "LOAD", "return redis.call('set','rk','v')"),
		cmd("EVALSHA_RO", "67372e76f490973b7669649bca9fad9c4d8ea17b", "0"),
		cmd("EVALSHA", "67372e76f490973b7669649bca9fad9c4d8ea17b", "0"), // write allowed via EVALSHA
		cmd("GET", "rk"),
		cmd("SCRIPT", "FLUSH"),
		cmd("DEL", "rok", "rk"),
	}},
}

var m8CallScripts = []script{
	{"eval-redis-call-basics", []step{
		cmd("EVAL", "return redis.call('set',KEYS[1],ARGV[1])", "1", "k", "v"),
		cmd("GET", "k"),
		cmd("EVAL", "return redis.call('get','k')", "0"),
		cmd("EVAL", "return redis.call('incr','ctr')", "0"),
		cmd("EVAL", "return redis.call('lpush','l1','a','b')", "0"),
		cmd("EVAL", "return redis.call('lrange','l1','0','-1')", "0"),
		cmd("EVAL", "return redis.call('get','missing')", "0"),
		cmd("EVAL", "return redis.call('get','missing') == false", "0"),
		cmd("EVAL", "return redis.call('set','k','v').ok", "0"),
		cmd("EVAL", "return redis.call('type','k')", "0"),
		cmd("EVAL", "redis.call('hset','h1','f','v') return redis.call('hgetall','h1')", "0"),
		cmd("EVAL", "return redis.call('hgetall','missing')", "0"), // empty map → *0
		cmd("EVAL", "return redis.call('dbsize')", "0"),
		cmd("EVAL", "return redis.call('ping')", "0"),
		cmd("EVAL", "return redis.call('echo','hey')", "0"),
		cmd("EVAL", "return redis.call('SET','upk','v')", "0"), // uppercase names
		cmd("EVAL", "return redis.call('get','upk')", "0"),
		cmd("EVAL", "return redis.call('zadd','z1','1.5','zm')", "0"),
		cmd("EVAL", "return redis.call('zscore','z1','zm')", "0"), // bulk in RESP2 mode
		cmd("EVAL", "return #redis.call('keys','*') >= 0", "0"),
		cmdM(mSetCmp, "EVAL", "return redis.call('keys','*')", "0"),
	}},
	{"eval-redis-call-arg-rendering", []step{
		cmd("EVAL", "redis.call('set','nk',1.5) return redis.call('get','nk')", "0"),
		cmd("EVAL", "redis.call('set','nk',1e30) return redis.call('get','nk')", "0"),
		cmd("EVAL", "redis.call('set','nk',0.1) return redis.call('get','nk')", "0"),
		cmd("EVAL", "redis.call('set','nk',-7) return redis.call('get','nk')", "0"),
		cmd("EVAL", "redis.call('set','nk',2^53) return redis.call('get','nk')", "0"),
		cmd("EVAL", "redis.call('set','nk',1/3) return redis.call('get','nk')", "0"),
		cmd("EVAL", "redis.call('set','nk',0.1^2) return redis.call('get','nk')", "0"),
		cmd("EVAL", "redis.call('set','nk',-0.0) return redis.call('get','nk')", "0"),
		cmd("EVAL", "redis.call('set','nk',1e15) return redis.call('get','nk')", "0"),
		cmd("EVAL", "redis.call('set','nk',123456789012345678) return redis.call('get','nk')", "0"),
		cmd("EVAL", "return redis.call('set', 1, 2)", "0"), // numeric key/value
		cmd("GET", "1"),
	}},
	{"eval-redis-call-errors", []step{
		cmd("EVAL", "return redis.call()", "0"),
		cmd("EVAL", "return redis.call('get')", "0"),
		cmd("EVAL", "return redis.call('set','k')", "0"),
		cmd("EVAL", "return redis.call(true,'x')", "0"),
		cmd("EVAL", "return redis.call({})", "0"),
		cmd("EVAL", "return redis.call('set','k',{})", "0"),
		cmd("EVAL", "return redis.call('set','k',nil)", "0"),
		cmd("EVAL", "return redis.call('set','k',true)", "0"),
		cmd("EVAL", "return redis.call('nosuchcmd','x')", "0"),
		cmd("EVAL", "return redis.call('  set  ','spk','v')", "0"), // no trimming
		cmd("EVAL", "return redis.call('eval','return 1','0')", "0"),
		cmd("EVAL", "return redis.call('evalsha','x','0')", "0"),
		cmd("EVAL", "return redis.call('script','load','return 1')", "0"),
		cmd("EVAL", "return redis.call('subscribe','c')", "0"),
		cmd("EVAL", "return redis.call('psubscribe','c*')", "0"),
		cmd("EVAL", "return redis.call('multi')", "0"),
		cmd("EVAL", "return redis.call('exec')", "0"),
		cmd("EVAL", "return redis.call('watch','k')", "0"),
		cmd("EVAL", "return redis.call('config','get','maxmemory')", "0"),
		cmd("EVAL", "return redis.call('client','setname','x')", "0"),
		cmd("EVAL", "return redis.call('auth','x')", "0"),
		cmd("EVAL", "return redis.call('monitor')", "0"),
		cmd("EVAL", "return redis.call('save')", "0"),
		cmd("EVAL", "return redis.call('bgsave')", "0"),
		cmd("EVAL", "return redis.call('hello','3')", "0"),
		cmd("EVAL", "local a = 1\nlocal b = 2\nreturn redis.call('get')", "0"), // line 3
		cmd("EVAL", "return redis.pcall()", "0"),
		cmd("EVAL", "return redis.pcall('get')", "0"),
		cmd("EVAL", "return redis.pcall('nosuchcmd')", "0"),
		cmd("EVAL", "return redis.pcall('eval','return 1','0')", "0"),
		cmd("EVAL", "return type(redis.pcall('get'))", "0"),
		cmd("EVAL", "return redis.sha1hex()", "0"),
		cmd("EVAL", "return redis.sha1hex('a','b')", "0"),
		cmd("EVAL", "return redis.error_reply('my err')", "0"),
		cmd("EVAL", "return redis.status_reply('my status')", "0"),
		cmd("EVAL", "return redis.status_reply('')", "0"),
		cmd("EVAL", "return redis.error_reply()", "0"),
		cmd("EVAL", "return redis.status_reply(42)", "0"),
	}},
	{"eval-redis-call-blocking", []step{
		cmd("EVAL", "return redis.call('blpop','emptylist',0)", "0"), // null, never blocks
		cmd("EVAL", "return redis.call('blpop','emptylist',1)", "0"),
		cmd("RPUSH", "bl1", "x"),
		cmd("EVAL", "return redis.call('blpop','bl1',0)", "0"), // pops immediately
		cmd("EVAL", "return redis.call('bzpopmin','ez',0)", "0"),
		cmd("ZADD", "bz1", "1", "zm"),
		cmd("EVAL", "return redis.call('bzpopmin','bz1',0)", "0"),
	}},
	{"eval-redis-call-allowed-admin", []step{
		cmd("SET", "victim", "1"),
		cmd("EVAL", "return redis.call('flushall')", "0"),
		cmd("DBSIZE"),
		cmd("EVAL", "return redis.call('flushdb')", "0"),
		cmd("SET", "after", "1"),
		cmd("EVAL", "return redis.call('publish','chan','msg')", "0"),
		cmd("DEL", "after"),
	}},
	{"eval-setresp", []step{
		cmd("HSET", "h1", "f1", "v1"),
		cmd("SADD", "s1", "m1"),
		cmd("ZADD", "z1", "1.5", "zm"),
		cmd("EVAL", "redis.setresp(3) local t = redis.call('hgetall','h1') return t.map.f1", "0"),
		cmdM(mSetCmp, "EVAL", "redis.setresp(3) return redis.call('hgetall','h1')", "0"),
		cmd("EVAL", "redis.setresp(3) local t = redis.call('smembers','s1') return t.set.m1", "0"),
		cmd("EVAL", "redis.setresp(3) local t = redis.call('zscore','z1','zm') return t.double", "0"),
		cmd("EVAL", "redis.setresp(3) return redis.call('zscore','z1','zm')", "0"),
		cmd("EVAL", "redis.setresp(3) return redis.call('get','missing') == nil", "0"),
		cmd("EVAL", "redis.setresp(3) return redis.call('get','missing') == false", "0"),
		cmd("EVAL", "redis.setresp(3) return redis.call('get','missing')", "0"), // nil → null reply
		cmd("EVAL", "redis.setresp(3) return redis.call('incr','c9')", "0"),
		cmd("EVAL", "redis.setresp(3) redis.call('set','x','1') return redis.call('type','x')", "0"),
		// Return rendering follows the run's reply version: false is a
		// RESP3 boolean under 3 (:0 on a RESP2 client), null under 2.
		cmd("EVAL", "redis.setresp(3) return false", "0"),
		cmd("EVAL", "redis.setresp(3) return true", "0"),
		cmd("EVAL", "redis.setresp(3) return {false,true}", "0"),
		cmd("EVAL", "redis.setresp(3) redis.setresp(2) return false", "0"),
		cmd("EVAL", "redis.setresp(3) redis.setresp(2) return redis.call('get','missing') == false", "0"),
		cmd("EVAL", "redis.setresp(2) redis.setresp(3) return false", "0"),
		cmd("EVAL", "redis.setresp(2) return redis.call('hgetall','h1')", "0"),
		cmd("EVAL", "return redis.setresp(3)", "0"), // no return → null
		cmd("EVAL", "return redis.setresp()", "0"),
		cmd("EVAL", "return redis.setresp(4)", "0"),
		cmd("EVAL", "return redis.setresp('3')", "0"),
		cmd("EVAL", "return redis.setresp('x')", "0"),
		cmd("DEL", "h1", "s1", "z1", "c9", "x"),
	}},
}

var m8FlowScripts = []script{
	{"eval-in-multi", []step{
		cmd("MULTI"),
		cmd("EVAL", "return redis.call('set','tx1','a')", "0"),
		cmd("EVAL", "return 1", "0"),
		cmd("SCRIPT", "LOAD", "return 7"), // SCRIPT queues too
		cmd("EXEC"),
		cmd("GET", "tx1"),
		cmd("EVALSHA", "59b6ab2fbe0ee4b25733de0f62e6cda4899ef8e9", "0"), // sha1("return 7")
	}},
	{"eval-multi-errors", []step{
		cmd("MULTI"),
		cmd("EVAL", "return 1", "x"), // queue-time error dirties the tx
		cmd("EXEC"),
		cmd("GET", "k"),
	}},
	{"eval-busy-and-kill", []step{
		cmd("CONFIG", "SET", "lua-time-limit", "100"),
		sendOn(1, "EVAL", "while true do end", "0"),
		// SLEEP 2000, not 400: under -race the fresh-VM instantiation can
		// eat most of a 400 ms window before the loop even starts, and the
		// BUSY gate would not be up yet (flakes; ultima #M8d).
		cmd("SLEEP", "2000"),
		cmd("GET", "anything"),   // BUSY
		cmd("PING"),              // BUSY
		cmd("INFO"),              // BUSY
		cmd("SCRIPT", "KILL"),    // allowed through the gate
		recvOn(1, mEq),           // the killed script's error
		cmd("SCRIPT", "KILL"),    // NOTBUSY now
		cmd("SET", "after", "1"), // healthy again
		cmd("CONFIG", "SET", "lua-time-limit", "5000"),
		cmd("DEL", "after"),
	}},
	// UNKILLABLE is not differential-testable: the write-having script
	// must run long enough to observe BUSY on both engines, and a CPU
	// loop runs >100x slower on the wasm backend than on PUC Lua (no
	// workload spans both timing windows, and Redis cannot be
	// preempted once it wrote). Covered Ultima-side by
	// tests/m8_script_test.go (TestM8UnkillableAfterWrite).
	{"eval-pubsub-from-script", []step{
		cmdOn(1, "SUBSCRIBE", "scriptchan"),
		cmd("EVAL", "return redis.call('publish','scriptchan','hello')", "0"),
		expectPush(1, mEq),
		cmdOn(1, "UNSUBSCRIBE"),
	}},
	{"eval-config-lua-time-limit", []step{
		cmdM(mConfigPairs, "CONFIG", "GET", "lua-time-limit"),
		cmd("CONFIG", "SET", "lua-time-limit", "abc"),
		cmd("CONFIG", "SET", "lua-time-limit", "-1"),
		cmd("CONFIG", "SET", "lua-time-limit", "0"),
		cmdM(mConfigPairs, "CONFIG", "GET", "lua-time-limit"),
		cmd("CONFIG", "SET", "lua-time-limit", "5000"),
	}},
}
