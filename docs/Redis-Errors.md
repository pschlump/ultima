# Redis 7.2.7 scripting (EVAL) — probed error texts and conversion rules

Probed against a live `redis-server` 7.2.7 (the compat target,
`commands.CompatVersion`) on 2026-09-22, protocol-verified with raw RESP
reads (RESP2 and RESP3 legs) — redis-cli pretty-printing hides types, so
every entry below shows exact wire bytes where it matters. These probes
feed the M8 Lua integration (`lib/scripting`, `lib/commands/eval.go`) and
the differential corpus (`tests/differential/scripts_m8.go`). When this
file and live Redis disagree, live Redis wins and this file gets fixed.

## 1. EVAL / EVALSHA argument validation (in this order)

| Input | Reply |
|---|---|
| `EVAL "return 1" x` (bad numkeys) | `ERR value is not an integer or out of range` |
| `EVAL "return 1" -1` | `ERR Number of keys can't be negative` |
| `EVAL "return 1" 2 a` (numkeys > args) | `ERR Number of keys can't be greater than number of args` |
| `EVAL "return 1" 1` (same — no key args) | `ERR Number of keys can't be greater than number of args` |
| `EVAL "return 1"` | `ERR wrong number of arguments for 'eval' command` |
| `EVALSHA deadbeef 0` | `NOSCRIPT No matching script. Please use EVAL.` |
| `EVALSHA xyz 0` (malformed sha) | `NOSCRIPT No matching script. Please use EVAL.` (no validation of the sha form) |

`numkeys 0` with extra args puts the extras in `ARGV` (`EVAL "return #ARGV" 1 k a1 a2` → 2;
`KEYS[1]` past numkeys is nil → null reply).

## 2. SCRIPT subcommands

| Input | Reply |
|---|---|
| `SCRIPT` | `ERR wrong number of arguments for 'script' command` |
| `SCRIPT FOO` | `ERR unknown subcommand 'FOO'. Try SCRIPT HELP.` (subcommand echoed in the client's casing) |
| `SCRIPT LOAD` (no body) | `ERR wrong number of arguments for 'script|load' command` |
| `SCRIPT LOAD 'return 42'` | bulk, 40-hex sha1 of the source |
| `SCRIPT EXISTS <sha> deadbeef` | array of 0/1 per sha; non-sha strings just get 0 |
| `SCRIPT EXISTS` (no args) | `ERR wrong number of arguments for 'script|exists' command` |
| `SCRIPT FLUSH` / `ASYNC` / `SYNC` | `+OK` |
| `SCRIPT FLUSH BADMODE` | `ERR SCRIPT FLUSH only support SYNC|ASYNC option` |
| `SCRIPT FLUSH async extra` | same SYNC\|ASYNC error (not the arity form) |
| `SCRIPT KILL` (nothing running) | `NOTBUSY No scripts in execution right now.` |
| `SCRIPT DEBUG yes` | `+OK` (7.2.7 ships the Lua debugger; Ultima does not implement it) |
| `SCRIPT HELP` | array of help lines as SIMPLE strings (see `lib/commands/script.go`) |
| `EVALSHA` of a flushed script | `NOSCRIPT …`; `SCRIPT FLUSH` while a script runs is tolerated (only future lookups miss) |

## 3. Script compile / runtime errors

Compile errors:

```
ERR Error compiling script (new function): user_script:1: '=' expected near 'is'
ERR Error compiling script (new function): user_script:1: unexpected symbol near '<eof>'
ERR Error compiling script (new function): user_script:1: chunk has too many syntax levels   (~1000 nested '{')
```

Runtime errors carry a standard suffix: the message, then
` script: <sha1hex>, on @user_script:<line>.` — the line is the *raise* line
(debug info), independent of whether the message text itself has a position
prefix. Rules:

| Script | Reply (suffix elided as `… script: <sha>, on @user_script:N.`) |
|---|---|
| `error('boom')` | `ERR user_script:1: boom …` — strings get `ERR ` + the error()-positioned message |
| multiline `error("boom3")` at line 3 | `ERR user_script:3: boom3 … on @user_script:3.` |
| `error('positional', 0)` | `ERR positional …` (level 0: no position in the message; suffix still tracks the raise line) |
| `error('WRONGTYPE foo')` | `ERR user_script:1: WRONGTYPE foo …` (`ERR ` is prepended even to code-prefixed strings) |
| `error(42)` | `ERR user_script:1: 42 …` (numbers are tostring-able: position prefix applies) |
| `error(42, 0)` | `ERR 42 …` |
| `error(true)` / `error(nil)` | `ERR true …` / `ERR nil …` (no position for non-string-convertible values) |
| `error({err='x'})` / `error({err='x', code=5})` | `x script: …` — table errors with a **string** `err` field pass verbatim: no `ERR ` prefix, no position in the text |
| pcall'd error re-raised via `error(r)` | same table rule |

**Redis 7.2.7 CRASH INPUTS (verified, server dies — SIGSEGV-class, connection
refused afterwards):**

- `EVAL "return error({})" 0`
- `EVAL "return error({code=5})" 0` — any error() table **without** an `err` field
- `EVAL "local t = setmetatable({}, {__tostring=function() return 'MTSTR' end}) return error(t)" 0`

Ultima must never die on these (the `Engine.Execute` no-panic contract);
they are differential skip-with-reason rows, not parity targets.

## 4. Lua → RESP conversions (script return values)

| Lua value | RESP2 wire | RESP3 wire |
|---|---|---|
| number, integral (`1`, `2^53`, `1e15`) | `:N` | same |
| number, non-integral (`1.5`, `0.1`) | **truncated toward zero**: `1.5→:1`, `-1.5→:-1`, `0.1→:0` | same |
| `-0` | `:0` | same |
| overflow / `math.huge` / `1e30` / `2^63` | clamped `:9223372036854775807` | same |
| `-math.huge` / `-2^63` | `:-9223372036854775808` | same |
| `0/0` (NaN) | `:0` | same |
| string (binary-safe, NULs fine) | `$n` | same |
| `true` | `:1` | `:1` |
| `false` | `$-1` | `_` |
| `nil` | `$-1` | `_` |
| function | `$-1` | `_` |
| `{}` (empty) | `*0` (empty array — NOT null) | same |
| `{a=1}`, `{[0]='z',[1]='a'}`, `{[-1]=..}`, `{[1.5]=..}` | only keys `1..n` count, non-positive/non-integral ignored → `{a=1}` is `*0` | same |
| `{1,nil,3}` (hole) | truncated at the hole: `*1 :1` | same |
| `{[2]='b'}` (no `[1]`) | `*0` | same |
| nested tables | recursive; depth 100 works | same |
| `{err='text'}` | `-text` (verbatim, no `ERR ` added) | same |
| `{ok='text'}` | `+text` (simple string; `status_reply('')` → `+`) | same |
| `{map={...}}` | flat array of the inner pairs (`*2 a 1`) | `%n` map of the pairs |
| `{set={...}}` | array of the inner KEYS (values ignored) | `~n` set of the keys |
| `{double=2.5}` | bulk of Redis's double render (`2.5`, `2`, `nan`) | `,2.5` double |
| `{ok='a', err='b'}` | `-b` (`err` beats `ok`; `ok` beats `map`; `map` beats `set`) | same |
| `{err='x', 1}` | `-x` | same |
| `{err=42}`, `{ok=42}`, `{err=true}` | not status/error tables → plain array rule → `*0` | same |
| `{map='str'}`, `{set='str'}`, `{double='x'}` | non-table/non-number wrapper → `*0` | same |
| nested wrapper tables | `{{err='x'}}` → `*1 -x` (error/simple ELEMENTS at any depth); `{map={a={err='e'}}}` → `a` → `-e`; `{map={x={map={y=1}}}}` recurses | same |
| `return 1,2,3` | only the FIRST value is used | same |

**Return rendering follows the run's reply version** (`redis.setresp`):
under RESP 2, `true` → `:1` and `false` → null; under RESP 3, `true`/`false`
become RESP3 booleans (`#t`/`#f` — which a RESP2 *client* sees as `:1`/`:0`).
`nil` is null either way. The `{map}/{set}/{double}` wrapper rules apply in
both modes.

## 5. redis.call / redis.pcall

Argument rules: every argument must be a string or a number; anything else
(table, boolean, nil) → error. Numbers are rendered: integral and in int64
range → plain integer decimal (`1e15→"1000000000000000"`,
`2^53→"9007199254740992"`, `123456789012345678→"123456789012345680"` (f64
rounding), `-0.0→"0"`); otherwise shortest-round-trip decimal
(`1/3→"0.3333333333333333"`, `0.1^2→"0.010000000000000002"`, `1e30→"1e+30"`).

Error texts (raised by `redis.call`; all carry the
` script: <sha>, on @user_script:N.` suffix with N = the call's line):

| Case | Text |
|---|---|
| `redis.call()` | `ERR Please specify at least one argument for this redis lib call` |
| inner arity (`redis.call('get')`) | `ERR Wrong number of args calling Redis command from script` |
| non-string/number arg (`true`, `{}`, `nil`) | `ERR Lua redis lib command arguments must be strings or integers` |
| unknown command | `ERR Unknown Redis command called from script` |
| `eval`/`script`/`subscribe`/`multi`/`watch`/`config` from a script | `ERR This Redis command is not allowed from script` |
| `redis.sha1hex()` (no args) | `ERR wrong number of arguments` |
| `redis.error_reply()` / `redis.status_reply()` (no/bad args, e.g. `42`) | `ERR wrong number or type of arguments` (NO script suffix — these are direct replies, not raised) |
| `EVAL_RO` running a write | `ERR Write commands are not allowed from read-only scripts.` |

`redis.pcall` returns the same failures as a Lua table
`{err="<text>", ignore_error_stats_update=1}` (same texts, no suffix);
returning that table to the client yields `-ERR <text>` without the
suffix. **Raised error values are STRINGS, not tables**: `redis.call`
raises the error text as a string carrying an error-object metatable
(`pcall(function() redis.call(...) end)` catches
`type(err) == "string"`, `tostring(err)` is the text, `err.err` is nil).
Redis's own patched `error()` also stringifies table arguments:
`error({err='tbl'})` raises a string. The metatable is what marks the
text as verbatim-class in replies (Ultima cannot reproduce it — see §11).
Allowed from scripts: `flushall`,
`publish`, `blpop` (runs **non-blocking**: pops if data, null reply if
empty), uppercase command names OK, `'  set  '` (spaces) is an unknown
command.

`redis.error_reply(s)`: returns `{err=s}` — **with `"ERR "` prepended
when s contains no space** (`'x'` → `"ERR x"`, `'x y'` → `"x y"`,
`'ERR'` → `"ERR ERR"`, `''` → `"ERR "`). `redis.status_reply(s)` returns
`{ok=s}` verbatim.

`redis.setresp(2|3)` (errors: no args → `ERR redis.setresp() requires
one argument.`; bad version → `ERR RESP version must be 2 or 3.` — both
raised, with the script suffix; a numeric string `'3'` is accepted).
Under RESP 3, command replies convert: null → **nil** (not false;
`type(...)` is `"nil"`), doubles → `{double=n}`, maps → `{map={k=v,…}}`,
sets → `{set={member=true,…}}` (a Lua table with a single wrapper field
— `t.f1` on an hgetall result is nil, `t.map.f1` works). Switching back
with `redis.setresp(2)` restores the RESP 2 forms.

RESP → Lua (what the script sees): integer → number; bulk/simple → string,
except status replies of write commands become `{ok="OK"}`
(`redis.call('set','k','v').ok == "OK"`); null → **`false`**
(`redis.call('get','missing') == false` → true); array → table 1..n;
error → raised (`call`) / `{err}` table (`pcall`). `redis.setresp(3)` makes
aggregate replies maps (`hgetall` → `%1` map reply when returned).

## 6. The `redis` table surface (7.2.7)

`call`, `pcall`, `error_reply`, `status_reply`, `sha1hex`, `log`,
`setresp`, `breakpoint` are functions. `LOG_DEBUG/VERBOSE/NOTICE/WARNING` =
0/1/2/3. `REDIS_VERSION` = `"7.2.7"`. `redis.log(level, msg)` writes to the
server log.

## 6a. Globals lockdown (probed 7.2.7; implemented M8d)

Redis runs scripts with the globals table locked (script_lua.c +
deps/lua's `readonly` table patch). Ultima's gopher-lua runtime ports the
identical mechanism (Table.readonly flag checked in `luaV_settable` /
`lua_rawset` / `lua_rawseti`, the `__index` error metatable, recursive
protection of every table reachable from `_G`), so these are byte-exact:

| Script | Reply (all runtime errors carry the ` script: <sha>, on @user_script:N.` suffix) |
|---|---|
| `return undefined_global` | `ERR user_script:1: Script attempted to access nonexistent global variable 'undefined_global'` |
| `g = 5 return g` (create global) | `ERR user_script:1: Attempt to modify a readonly table` |
| `redis = nil` / `redis.error_reply = nil` / `string.foo = 1` / `math.huge = 5` / `_G.pairs = nil` | same readonly text (recursive protection) |
| `rawset(_G,'g3',1)` / `rawseti(_G,1,'x')` | `ERR Attempt to modify a readonly table` — **no position prefix** (the raise runs inside the C base function, so no Lua line is attributed) |
| `return _G[nil]` | `ERR user_script:1: Second argument to luaProtectedTableError must be a string or number` |
| `return rawget(_G,'undefined_global')` | `(nil)` — rawget bypasses the `__index` guard |
| `KEYS[1] = 'x'` / `ARGV[1] = 'y'` | allowed (KEYS/ARGV are staged after the lockdown) |
| `local t={} rawset(t,'k',1)` | allowed (user tables are not readonly) |
| `pcall(function() return nosuchglobal end)` | catches; the message carries the `user_script:1: ` prefix |
| `return _G` | empty array reply |
| `return getmetatable(_G) ~= nil` | 1 (the guard metatable is visible) |
| `rawseti` | not a 5.1 builtin at all — the guard's nonexistent-global error fires first |

## 7. BUSY / SCRIPT KILL (probed with `lua-time-limit 100`)

| Situation | Reply |
|---|---|
| any normal command from a second client while a script runs past `lua-time-limit` | `BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.` |
| `SCRIPT KILL` of a script that has not written | `+OK`; the scripting client gets `ERR Script killed by user with SCRIPT KILL... script: <sha>, on @user_script:1.` |
| `SCRIPT KILL` after the script wrote | `UNKILLABLE Sorry the script already executed write commands against the dataset. You can either wait the script termination or kill the server in a hard way using the SHUTDOWN NOSAVE command.` |

Ultima divergence (decision S5): the `script_hard_deadline_ms` watchdog
kills even write-having scripts at the hard deadline, keeping their
(already AOF-captured) partial effects.

## 8. Command metadata (probed `COMMAND INFO`, 7.2.7)

| Command | Arity | Flags | Keys |
|---|---|---|---|
| `eval` / `evalsha` | -3 | `noscript`, `stale`, `skip_monitor`, `no_mandatory_keys`, `movablekeys` | 0/0/0 |
| `eval_ro` / `evalsha_ro` | -3 | `readonly` + same five | 0/0/0 |
| `script` | -2 | (none) | 0/0/0 |

Note: **no `write`/`denyoom` on EVAL** — EVAL itself is not OOM-gated; the
inner commands called via `redis.call` carry their own flags.

`CONFIG GET lua-time-limit` → `5000` (default, ms).

## 9. MONITOR and MULTI interplay

- MONITOR shows the EVAL argv verbatim, then each inner command on its own
  line with the address field `lua` and the command name **as the script
  wrote it** (lowercase in the probe):
  `+… [0 lua] "set" "mk" "mv"`.
- EVAL inside MULTI queues normally; EXEC runs it (script writes included).
- Redis cannot preempt a running script; only `SCRIPT KILL` (pre-write) or
  `SHUTDOWN NOSAVE` help.

## 10. INFO

7.2.7 has **no `# Script` INFO section** (sections: Server, Clients,
Memory, Persistence, Stats, Replication, CPU, Modules, Errorstats,
Cluster, Keyspace). Ultima adding one to plain INFO would break the
differential INFO matcher; scripting counters therefore do not belong in
the default INFO output.

## 11. Known Ultima-side divergence notes (for the M8 ledger)

- gopher-lua compile-error wording differs from PUC Lua
  (`user_script line:1(column:7) near 'is': parse error` vs
  `user_script:1: '=' expected near 'is'`) — shape matches
  (`ERR Error compiling script (new function): …`), details do not.
- Lua runtime-error texts follow the gopher-lua dialect
  (`attempt to call a non-function object`), not PUC/Redis texts
  (`attempt to call a nil value`). (The globals lockdown — Redis's
  `Script attempted to access nonexistent global variable 'x'` /
  `Attempt to modify a readonly table` — IS byte-exact since M8d; see
  §6a.)
- `tostring(0.1^2)` renders shortest-round-trip
  (`0.010000000000000002`) where PUC's `%.14g` gives `0.01`
  (gopher-lua divergence-ledger rows 38/40/48 family). Host-side reply
  formatting (decision S7) keeps this out of *numeric* replies; scripts
  that `tostring()` floats themselves observe the dialect.
- Deep table nesting: the guest wire encoder caps expansion (64 levels);
  Redis nests ≥100. A >64-deep return is a clean error in Ultima.
- `error(42, 0)`-style level-sensitive position rendering of **number**
  error values is not tracked (the staged value carries no level);
  Ultima renders number errors with the position (`error(42)` form).
- Redis marks raised `redis.call` error strings with an **error-object
  metatable**; Ultima cannot attach metatables to raised values, so the
  verbatim error class is recognized by text (an error-code word prefix
  with no position prefix). Collision: a level-0 user string starting
  with an error-code word (`error('ERR already', 0)`,
  `error('WRONGTYPE foo', 0)`, `error('OOM x', 0)`) renders verbatim
  where Redis renders `ERR <text>`.
- **Scripts do not share globals across EVAL calls** (decision S4):
  Redis runs all scripts in one global lua_State (`EVAL "g=42 return 1" 0`
  then `EVAL "return g" 0` → 42); Ultima's fresh-VM-per-run lifecycle
  gives every script a clean _G (nil). Pooled-VM-per-script deployment
  (the M8e performance item) can revisit this.
- `SCRIPT DEBUG` is refused (`ERR SCRIPT DEBUG is not supported by this
  server.`); 7.2.7 answers `OK` and enters ldb (the wasm backend exposes
  no debug hooks).
- The hard-deadline watchdog (S5): a script running past
  `script_hard_deadline_ms` is killed mid-loop with
  `ERR Script killed by the hard execution deadline
  (script_hard_deadline_ms)` and keeps its partial, already-AOF-captured
  effects. Redis cannot preempt; there is no parity text.
- The killed-script reply line for deadline-class errors is reported as
  1 when the runtime stages no line (gopher-lua ledger row 37).
- Performance: on darwin/arm64 the wazero engine runs the wasm backend
  interpreted — a `while i<N do i=i+1 end` loop is >100x slower than PUC
  Lua (Redis 7.2.7 does ~230M simple iterations/s; the backend ~1.1M/s).
  Correctness is unaffected; M8e benchmarks/trip-wires track this
  (integration-guide risk R3).
- `redis.pcall`'s error table carries `ignore_error_stats_update=1`
  (reproduced); the metatable on raised strings is not (above).
- `CONFIG GET *` exposes the Ultima-only `script-*` keys.
- No `-LOADING` gate: Redis accepts connections during dataset load and
  replies `LOADING Redis is loading the dataset in memory` to commands
  (including EVAL); Ultima's restore-before-serve (§13.1) binds the
  listeners but starts accepting only after restore, so a client can
  never observe the loading state. Deliberate M5c design, unchanged.
- Compile-error frontend messages end with a trailing newline inside
  gopher-lua; Ultima strips it (`CompileErrorReply`) so the RESP and
  binary surfaces render the same single-line error.
