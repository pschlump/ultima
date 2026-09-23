# M8 detailed plan — Lua scripting (EVAL) as built

Status: **implemented (2026-09-22)**. This documents the Lua-scripting
portion of M8 (design doc §14.4: "P3/P4 parity tail (streams, Lua-lite,
bitfield, geo, PF\*)" — the rest of the tail landed earlier). It is
written retro-accurately: it records what was actually built, probed, and
gated, not the plan. The working guide was
`../gopher-lua/docs/How-To-Integrate-Gopher-Lua-with-Redis-Clone-Called-Ultima.md`;
the probing ground truth is `docs/Redis-Errors.md` (every conversion and
error string there was verified against live Redis 7.2.7 at the protocol
level).

## Scope delivered

- `EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO`
- `SCRIPT LOAD|EXISTS|FLUSH [ASYNC|SYNC]|KILL|HELP`
  (`SCRIPT DEBUG` refused — ledgered)
- `redis.call` / `redis.pcall` / `redis.error_reply` /
  `redis.status_reply` / `redis.sha1hex` / `redis.log` / `redis.setresp`
  plus `redis.LOG_*` constants and `redis.REDIS_VERSION`
- Atomicity under `PauseAll` (S3, the EXEC model), effects-only AOF
  capture per inner command (S2), the soft BUSY phase
  (`lua-time-limit`) + `SCRIPT KILL`, the hard watchdog deadline (S5),
  the per-VM memory budget, host-seeded deterministic RNG (S6),
  host-side number formatting (S7), the Redis conversion rules (probed
  byte-exact, including the `{map=}/{set=}/{double=}` wrapper tables and
  the `setresp` modes), sandbox (prod blob: no WASI, no io/os/debug,
  loadstring nil)
- gRPC + WS surfaces via the generic envelope (no typed messages), CLI
  matrix cases deferred to M8d

## Architecture (as built)

```
Execute → table["eval"] → cmdEval (lib/commands/eval.go)
   compile-or-cache (SHA-1, lib/scripting)
   NewVM  (BEFORE the pause — instantiation is the slow part)
   PauseAll (S3; skipped when cs.tok != 0 — EVAL inside EXEC reuses the
             transaction's token; txMu is not reentrant)
   scripting.Manager.RunOnVM  (marks the run: BUSY gate + SCRIPT KILL)
     host.VM.Run  (wazero + lua51_prod.wasm + script.wasm)
       redis.call → host.host_call → bridge (lib/scripting/bridge.go)
         → commands.runScriptCommand (S9 gates, capturePersist,
            [db lua] monitor feed) — same inner path as cmdExec
   ToReply (probed conversion rules, run's final setresp version)
```

Decisions S1–S10 from the integration guide all stand. Notable as-built
points:

- **gopher-lua additions** (tagged v0.0.3 work, repo
  `../gopher-lua`): the M7a `host/` package plus three M8 additions:
  `rt_err_line` (the raise line captured at first error staging —
  Redis's `on @user_script:N` suffix), `host.ScriptError.Line`,
  `VM.Kill()` (SCRIPT KILL trips the same deadline flag as the
  watchdog), `Engine.RegisterValue` (Lua constants: `redis.LOG_*`,
  `REDIS_VERSION`), and `host.ValueError` (a HostFunc raising a
  non-string Lua error value — the redis.call error STRING class).
  Blob rebuilt; SHA-256 pin `27d9802a…`; all gopher-lua gates green.
- **redis.call raises a plain error STRING** (`host.ValueError`),
  mirroring Redis's metatagged error string: `pcall(redis.call)`
  catches `type(err)=="string"` exactly like Redis. `redis.pcall`
  returns `{err=..., ignore_error_stats_update=1}`. Since Ultima cannot
  attach the metatable, the reply layer recognizes the verbatim error
  class by an error-code-word prefix (`isErrorCodePrefixed`,
  `lib/scripting/convert.go`) — the ledgered collision is level-0 user
  strings starting with an error-code word.
- **Scripts are compiled under chunk name `user_script`** so runtime
  error messages render exactly like Redis's (PUC strips the `@` of
  `@user_script`; the gopher frontend renders the name verbatim).
- **CONFIG**: `lua-time-limit` (live, byte-exact SET errors) plus
  Ultima-only `script-hard-deadline-ms` (immutable at runtime),
  `script-max-memory-mb` (immutable), `script-rng-seed` (live), and the
  M8e R3 pool knobs `script-vm-pool-size` / `script-vm-pool-max` /
  `script-vm-recycle-runs` / `script-vm-recycle-pct` (all read-only).
  Config group `script` in `lib/config`.
- **INFO**: no `# Script` section in plain INFO (7.2.7 has none; the
  differential mInfo gate compares section sets) — scripting counters
  only under explicit `INFO script`.
- **Flush fan-out fix**: `FLUSHDB/FLUSHALL/DBSIZE` fan out shard tasks;
  under a pause (EXEC or EVAL) the fan-out must carry the caller's
  token — new `shard.FlushDBTok/FlushAllTok/DBSizeTok`. This was a
  latent deadlock for `MULTI; FLUSHALL; EXEC` too, smoked out by
  `redis.call('flushall')`.

## Test gates (all green)

- `lib/scripting` conversion unit tests (probed golden values).
- `lib/commands/eval_test.go`: validation orders, error suffixes (incl.
  multiline raise lines), SCRIPT subcommand texts, redis.call gates,
  EVAL_RO, EVAL-in-MULTI, monitor `[db lua]` form, flush fan-out in
  MULTI.
- `tests/m8_script_test.go`: EVAL over RESP/gRPC/WS, BUSY + SCRIPT KILL
  (two connections), UNKILLABLE after write, hard deadline, memory cap,
  concurrent EVAL/normal traffic (`-race` clean).
- `tests/differential/scripts_m8.go`: ~25 scripts, 280+ comparisons,
  byte-exact incl. error strings; RESP3 leg. New `TestDifferentialM8`.
- `tests/m8_aof_test.go`: SIGKILL crash recovery of script effects
  (set/incr/lpush/set-PX→PXAT/blocking-pop→LPOP/SPOP→SREM/EVAL-in-
  MULTI/db-3); asserts no EVAL verb appears in the AOF (S2).
- `make lint` clean; `CGO_ENABLED=0 go build ./...` (S1 purity);
  full `go test ./...` green.

## Divergences (ledgered in docs/Redis-Errors.md §11)

Compile-error wording, Lua runtime-error dialect texts, float
`tostring()` dialect, >64-deep return tables, `error(42, 0)` number
position, the error-object metatable (level-0 code-prefixed user
strings), **no shared globals across EVAL calls** (S4), `SCRIPT DEBUG`
refusal, the hard-deadline kill text (S5), wazero interpreter
performance on darwin/arm64 (>100x on CPU loops — M8e tracks), the
extra `script-*` CONFIG keys.

## Remaining (M8d/M8e)

- M8d status:
  - CLI-matrix cases for the five commands — DONE
    (`tests/cli-matrix/cases/90-scripting.txt`, 16 cases, green with
    `-R`; the stale `unknown.eval/evalsha/script` pins removed and
    COMMAND COUNT bumped 141 → 146).
  - Globals lockdown parity — DONE (found by the matrix): the Redis
    deps/lua readonly-table patch + `_G` error metatable ported into the
    gopher-lua runtime (`rt_protect_globals` / `rt_globals_readonly`,
    host option `WithGlobalsProtection`, byte-exact texts incl. the
    position-less rawset raise — docs/Redis-Errors.md §6a; differential
    rows in `eval-globals-guard`, host tests in
    `host/globals_protection_test.go`).
  - Loading-state gate — NOT NEEDED: restore-before-serve (§13.1) means
    no client can observe the loading state (ledgered in §11).
  - CLIENT KILL of a scripting connection — DONE
    (`TestM8ClientKillMidScript`: kill closes the socket, the
    unkillable run continues to the hard deadline, effects persist).
  - Compile-error trailing newline — FIXED (the gopher frontend's
    trailing `\n` rendered as a stray blank line on the binary
    surfaces; stripped in `CompileErrorReply`).
  - Overnight mixed-corpus soak — harness DONE (`tests/m8_soak_test.go`,
    `SOAK=1 SOAK_SECONDS=… go test ./tests -run TestM8Soak`: 8 workers
    over a 10-leg mixed corpus with per-leg reply-class assertions,
    randomized lua-time-limit, RSS trip-wire; 20–30 s smoke runs green).
    The overnight run itself is an operator action before release.
- M8e: EVAL throughput benchmarks vs 7.2.7 (`examples/bench` numbers,
  fresh-VM vs pooled), trip-wires, benchmark report in
  `docs/benchmarks/M8-<date>.md`. R3 (wazero interpreter speed) is the
  headline perf risk; a per-script VM pool is the sketched mitigation
  and would also enable Redis's shared-globals mode if ever wanted.
  - R3 VM pool — DONE (`lib/scripting/pool.go`): per-SHA pools of bound
    VMs (the host one-script law), checkout pre-pause / release post-run
    in `evalImpl`; recycling (guest GC is stopped, so heap grows
    monotonically) on run count (`script_vm_recycle_runs`, 100), heap
    watermark (`script_vm_recycle_pct`, 75% of the budget via the new
    `host.VM.UsedBytes` over the blob's `rt_mem_used_bytes`), fatal run
    errors (`IsVMFatal`: kill/hard-deadline/OOM/trap), stale epoch
    (SCRIPT FLUSH), and Close; per-SHA depth `script_vm_pool_size` (1;
    0 = pre-M8e fresh-VM path), global idle cap `script_vm_pool_max`
    (64, LRU). Found en route: `host.Engine.Compile`'s check-then-act
    race handed two `*Script` pointers for one source under concurrent
    EVAL (ErrScriptBound wedges) — fixed host-side and in
    `Manager.Compile`, regression tests in both repos. Reuse parity is
    gated by the `eval-vm-reuse` differential script (globals lockdown
    prevents cross-run leakage; docs/Redis-Errors.md §11).
