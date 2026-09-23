# Implementation History

The milestone-by-milestone record of how Ultima was built: what each
milestone delivered, the architecture decisions made along the way, and
the bugs/soaks that shaped the design. This file is **historical** — for
current operational guidance see `AGENTS.md`; for settled decisions see
`docs/ULTIMA-DESIGN.md` (§15, D1–D20). Milestone plans:
`docs/m5-detailed-plan.md`, `docs/m6-detailed-plan.md`,
`docs/m7-detailed-plan.md`, `docs/m8-detailed-plan.md`.

## Milestone status narrative (as of M8e, 2026-09-23)

M0 (skeleton, three listeners), M1 (shard engine, RESP front-end, P0
commands), M2 (P1 collections: hash/list/set/zset, differential-green on
H/L/S/Z) and M3 (P2: MULTI/EXEC/WATCH transactions, classic pub/sub,
blocking list/zset ops; differential-green) are implemented and
committed. M4 (binary front-ends) is **done**: the gRPC front-end (typed
Command envelope, Exec bidi stream, ExecBatch, ExecGeneric, Subscribe
pub/sub push stream, Monitor stream fed by `Engine.AddMonitor`) and the
WebSocket front-end (`lib/wssrv`, binary protobuf frames at `/ws/v1`,
pub/sub pushes as unsolicited seq-0 frames) both ride the shared
`lib/envelope` bridge. Exit criteria met: Go client round-trip (tests use
the generated gRPC client), TypeScript round-trip (`tests/ts-roundtrip`,
protobuf-es over `/ws/v1`, strict `tsc` typecheck), and a RESP↔gRPC
wire-byte parity gate (`tests/m4_parity_test.go`). The parity gate also
smoked out a P0 gap closed in M4: INCRBYFLOAT.

M5 (`docs/m5-detailed-plan.md`): **M5a** — keyspace notifications
(`notify-keyspace-events`, K/E classes on `__keyspace@<db>__`/
`__keyevent@<db>__` channels over the M3 broker, differential-green incl.
expiry events) and expiry hardening (time-boxed adaptive sweep). **M5b** —
maxmemory eviction (all 8 Redis policies, per-shard memory accounting and
eviction loops, the OOM gate with byte-exact `OOM`/`EXECABORT` replies;
differential-green on the gate strings). **M5c** — persistence
(`lib/persist`: own-format snapshot + per-shard AOF with global sequence
merge at replay, SAVE/BGSAVE/LASTSAVE/BGREWRITEAOF, restore-before-serve,
crash-recovery tested; M5c also closed the P0 gap EXPIREAT/PEXPIREAT,
needed by the AOF's PEXPIREAT rewrite). **M5d** — the maxmemory soak
(`bin/bench-m5.sh`, `make bench-m5`): sustained SET load at 20x keyspace
oversubscription against a bounded maxmemory under all 8 eviction
policies, gating on bounded `used_memory`, advancing `evicted_keys`, and
the byte-exact OOM/probe behavior; report in `docs/benchmarks/M5-<date>.md`;
memo in `note/M5-implemented.md`.

M6 (`docs/m6-detailed-plan.md`): **M6a** — the auth core (§9): `lib/auth`
(bcrypt account store with atomic JSON persistence, Ed25519 EdDSA JWT
access tokens per D20 via `golang-jwt/jwt/v5`, rotating refresh-token
families with theft detection, TOTP 2FA via `pschlump/htotp`), the
`/api/v1/auth/*` + `/api/v1/admin/users*` endpoints (now in
`lib/httpapi/auth_handlers.go`), Bearer gating of `/api/v1/*`, gRPC
interceptors (`authorization: bearer` metadata), and WS upgrade auth
(`?access_token=` or `Sec-WebSocket-Protocol: bearer, <token>`), all
behind the `auth.enabled` config gate (off = pre-M6 behavior). **M6b** —
resumable WS sessions (§9.4, D18): `lib/wssession` (per-session replay
buffer + `push_seq` handshake; subscription retention across drops;
SESSION_EXPIRED/ABORTED frames), wired into `lib/wssrv`. **M6c** — the
HTTP management API (§10.1, D7/D11): `api/openapi.yaml` is the contract
source of truth, regenerated into `gen/httpapi/` by `sh bin/gen-api.sh`
(`make gen_api`; also syncs the embed copy `lib/httpapi/openapi.yaml`);
`lib/httpapi` implements the generated chi-server bindings (auth routes
migrated from the deleted `lib/handler/auth.go`, plus /info, /shards,
/clients+kill, /slowlog, /latency, /config GET|PUT, /flushdb, /save
family, /keys/scan, /key/{key}, /metrics with the `server.metrics_allow`
IP allowlist), the spec is served at `/api/openapi.yaml` with Swagger UI
at `/api/docs`, and the middleware chain is request-id → logging →
Recoverer → Timeout → Prometheus → JWT gate (chi's deprecated RealIP
deliberately omitted — it would defeat the /metrics allowlist). **M6d** —
the web UI (§10.2): `web/` is a React+TS+vite app (bun; `make web` →
`web/dist`, embedded via `//go:embed` in `web/embed.go` — a committed
placeholder dist keeps bun-less builds working; served with SPA fallback
by `web.Handler()` on the chi `/*` catch-all) with login/TOTP, token
auto-refresh, dashboard, key browser, live monitor, slowlog, config
editor, pub/sub inspector, account admin (QR enrollment), and an
interactive console over `/ws/v1` with push-mode rendering and §9.4
session recovery. M6d also closed two follow-ups: the **MONITOR command**
(`lib/commands/monitor.go` — push-feed of other connections' commands,
needed by the web monitor screen) and the **WS origin policy**
(`server.ws_origin_allow`; enforced only when auth is enabled —
same-origin and no-Origin always pass).

**M7** — client libraries + examples (§11, `docs/m7-detailed-plan.md`):
the Go client `clients/go/ultima` (RESP/gRPC/WS/REST behind one umbrella
client, typed helpers, §9.4 session recovery on WS, shared token manager)
with the three operator CLIs (`cmd/ultima-cli`, `cmd/ultima-ws-cli`,
`cmd/ultima-grpc-cli` — thin shells over it, §6.4), the TypeScript client
`clients/typescript` (`@ultima/client` — the web UI was refactored onto
it, making it the §11.2 first consumer), the plain-JS ESM/CJS
distribution `clients/javascript`, and the leaderboard + chat example
apps under `examples/` (§11.4, e2e-tested in `tests/m7_examples_test.go`).

**M8 scripting** — Lua-lite (§7 P3, D12, `docs/m8-detailed-plan.md`):
EVAL/EVALSHA/EVAL_RO/EVALSHA_RO + SCRIPT LOAD/EXISTS/FLUSH/KILL/HELP on
the gopher-lua wasm engine (`lib/scripting` over `gopher-lua/host`, pure
Go, no cgo), atomic under PauseAll like EXEC, effects-only AOF capture
per `redis.call` (S2), BUSY phase + SCRIPT KILL, hard-deadline watchdog
(S5), per-VM memory budget, host-seeded RNG (S6), and the probed
byte-exact conversion/error surface (`docs/Redis-Errors.md` — RESP3
`setresp` wrapper tables included). Differential-green
(`tests/differential/scripts_m8.go`); crash-recovery tested
(`tests/m8_aof_test.go`). The rest of the M8 tail (streams, bitfield,
geo, PF*, SSUBSCRIBE) landed earlier. **M8e** added the R3 per-script VM
pool with periodic recycling (`lib/scripting/pool.go`; memo
`note/m8e-pool.md`) — EVAL went from ~200/s to 1.1–5.0k/s at -c 50.

## M3 architecture notes

- **Transactions**: MULTI queues on `ConnState`; EXEC takes
  `shard.Engine.PauseAll` (parks every shard goroutine in id order;
  only token-carrying tasks run — §4.2's strict cross-shard path) and
  runs the queue under the token. WATCH uses per-key `Entry.Version`
  counters plus per-shard delete tombstones and per-DB epochs
  (`Shard.WatchVersion`/`WatchDirty`); every in-place mutation path
  calls `Shard.Touch`.
- **Pub/sub**: `lib/pubsub` broker (no goroutines; delivery on the
  publisher's goroutine) → per-connection buffered push queues in
  `lib/respserver` drained by a writer goroutine via the fork's
  mutex-guarded `Conn.PushValue`; full queue = slow consumer →
  connection closed. Subscribe-mode gating (RESP2-only, like Redis) is
  in `Engine.Execute`.
- **Blocking**: waiters register in the owning shard's FIFO registry
  (check-and-register in one shard task, so no missed wakeups); pushes
  call `Shard.WakeWaiter`; the connection goroutine parks on a channel
  — never a shard goroutine. `shard.Engine.Closing()` unparks everyone
  on shutdown.

## M5a architecture notes

- **Keyspace notifications** (`lib/commands/notify.go`): `notify-keyspace-events`
  class bitmask on `commands.Engine` (parsed per Redis 7.2.7 letters
  `Ag$lshzxeKEtmdn`; CONFIG GET re-renders from the bitmask like Redis —
  `KEA` → `AKE`). Handlers call `Engine.notifyKeyspace(db, key, event)`
  after their shard closures return (deterministic arg order), EXCEPT
  commands whose mutation calls `Shard.WakeWaiter` (list pushes, LMOVE/BLMOVE
  dest, ZADD/ZINCRBY, and the blocking pops themselves): those publish
  inside the shard closure BEFORE waking, because the woken connection
  goroutine can otherwise pop and publish its own event first (Redis's
  single-threaded order guarantees pusher-event-before-woken-pop-event;
  the publish itself is broker-mutex-guarded and non-blocking, safe from
  a shard goroutine). It publishes over the M3 broker to
  `__keyspace@<db>__:<key>` (K, payload = event; published first, like
  Redis) and `__keyevent@<db>__:<event>` (E, payload = key). Expiry
  deletions reach it via `Shard.OnKeyGone` (reason `expired`; M5b
  reuses it for `evicted`). Known divergence: `SET k v PXAT <past>` —
  Ultima deletes inline at command time (no later lazy `expired` event;
  replies match Redis).
- **Expiry hardening**: `Shard.sweep` is time-boxed (`SweepTimeBox`, 1 ms)
  in addition to the `SweepMax` pop cap, and the cadence is adaptive —
  halving toward `SweepFloor` (10 ms) while sweeps stop with due work
  left, relaxing back to `SweepInterval` (100 ms) when clean
  (`nextSweepInterval`; the `active_expire_effort` analogue, §5.2).

## M5b architecture notes

- **Memory accounting**: `shard.Entry` caches `memBytes` (key + value +
  overhead estimate, D10 — monotone and comparable, never Redis-exact);
  collections track their content bytes incrementally
  (`lib/types/memusage.go`, O(1) `MemUsage()`). Choke points `Store`
  (new − old), `Touch` (recompute; signature is now
  `Touch(db, key, e)`), `Delete`/expiry (subtract) update per-DB
  `usedBytes` plus a shard-total rollup; `Engine.UsedBytes()` sums shard
  totals. FLUSHDB zeroes its DB share. INFO `used_memory` is the
  keyspace counter (Go heap moved to `used_memory_process`);
  `maxmemory_policy` is live; stats gained `evicted_keys`.
- **Eviction** (`lib/shard/evict.go`): `maybeEvict` runs after every
  shard task (run + park loops) while `maxmemory > 0` and policy ≠
  noeviction, evicting to the per-shard quota (maxmemory/shardCount).
  Always-on per-shard trackers keyed by `(db, key)`: exact LRU via pluto
  `lru_ts` (D14; `Oldest`/`PopOldest` added to pluto for this), LFU via
  `lfu_ts` Morris counters with `maxmemory-samples`-style candidate
  sampling (pluto `sharded_hash_ts.SampleStripe`, per-stripe since
  stripe i == shard i), volatile-* twins holding TTL'd keys only (added
  `Store`/`PushExpire`-via-`trackAccess`), volatile-ttl pops the expiry
  heap. FlushDB purges the flushed DB's entries from all four trackers
  AND rebuilds the expiry heap without them (M5d: a whole flushed DB's
  stale entries exhaust the small victim-validation budgets and wedge
  eviction); the remaining self-heal-at-validation path covers the
  slow-drip cases (TTL dropped on overwrite). Victims emit
  `evicted` via the M5a `OnKeyGone` sink; an overdue candidate is
  expired, not evicted.
- **OOM gate** (`lib/commands/engine.go`, probed against 7.2.7): over
  limit → denyoom commands get `OOM command not allowed when used memory
  > 'maxmemory'.`; EVERY command queued in MULTI is OOM-rejected at
  queue time (dirties EXEC → generic EXECABORT); EXEC of a tx containing
  a denyoom command aborts with the OOM reason embedded. Non-noeviction
  policies get a synchronous `Engine.EvictNow(cs.tok)` attempt first
  (performEvictions analogue) — rejection only when eviction can't get
  under the limit. EvictNow's verdict is per-shard, checked inside each
  shard goroutine right after its eviction pass: a global re-check races
  other connections' in-flight writes (a shard is legitimately over
  quota for the microseconds between a write's Store and its post-task
  maybeEvict — at saturation some shard is always in that window, so a
  global verdict spuriously OOMs under load; M5d soak found it).
  `maxmemory-policy` CONFIG with byte-exact enum error;
  `Engine.EvictSamples` = Redis maxmemory-samples default (5).

## M5c architecture notes

- **Snapshot** (`lib/persist/snapshot.go`, own format per D9): magic
  `ULTIMA01`, header, one segment per (db, shard) with CRC-64/XZ (pluto
  `crc`) per segment + whole-file trailer; payloads optionally
  LZW-compressed (pluto `quicklist.LZWCodec()`, `snapshot_compress`).
  Each segment is serialized inside its shard goroutine via
  `Shard.DumpDB` (walks only stripe i through pluto
  `sharded_hash_ts.StripeWalk`, skipping passively-expired keys) — a
  consistent per-shard point-in-time with no pause. SAVE takes
  `PauseAll` + `WriteSnapshotTok` (a tok-0 dump under the pause
  DEADLOCKS — parked shards stash non-token tasks); BGSAVE runs
  unstopped per-shard staggered tasks (divergence from fork-RDB,
  documented). tmp+rename atomicity, `snapshot.manifest` JSON sidecar.
- **AOF** (`lib/persist/aof.go`): per-shard logs `appendonlydir/
  shard-<i>.aof` + manifest. Every record is self-contained and
  seq-stamped — `["ULTIMAREC","<db>","<seq>",cmd,args...]` — seq from a
  set-wide atomic counter; broadcasts (FLUSHDB/FLUSHALL) share ONE seq
  across all logs. Replay (`replay.go`) merges all logs by seq
  (sort-merge over collected records) and executes each seq once — a
  file-at-a-time replay would re-execute broadcasts once per shard and
  clobber keys restored from earlier logs (found by unit test). Post-
  restart seqs resume above the replayed max. fsync: `always` syncs
  after each append, `everysec` via the manager's 1s ticker, `no`.
- **Capture** (`lib/commands/aof.go`): `Execute` reports every
  successful write-flagged command post-handler; EXEC is skipped there
  and `cmdExec` captures each queued command individually (7.2.7's AOF
  has no MULTI/EXEC framing — probed). Redis-exact rewrites (probed
  against live 7.2.7 AOF bytes): relative expires → absolute
  (`PEXPIREAT key abs [cond]` — condition kept; SET … EX/PX/EXAT →
  PXAT; GETEX → PEXPIREAT/PERSIST), blocking pops → plain effect on the
  replied key (BLPOP→LPOP, BLMOVE→LMOVE sans timeout, BLMPOP→1-key
  LMPOP, BZ* likewise), SPOP → SREM of the replied members, null/timeout
  → nothing. Expiry/eviction deletions arrive via `OnKeyGone` →
  synthesized DEL. v1 limitation: record order follows per-connection
  completion order; same-key writes racing across connections can
  replay in the other relative order (apply-time seq assignment is the
  M8 answer).
- **BGREWRITEAOF** dumps state as SET/RPUSH/SADD/ZADD/HSET + PEXPIREAT
  under PauseAll after draining write commands caught between mutation
  and capture (`commands.Engine.PersistQuiesced` — the
  persistInFlight/persistShardTasks/blockParked counters; EXEC excluded
  from tracking since it blocks on txMu). Stop-the-world rewrite — the
  documented divergence from fork+COW. Redis single-child rule mirrored:
  rewrite rejects BGSAVE (byte-exact "Another child process is
  active…" error), BGSAVE-in-progress makes BGREWRITEAOF reply
  "scheduled".
- **Restore/wiring**: `Manager.Start` restores synchronously BEFORE the
  serve loops start (listeners bound first, accepting after) — AOF when
  appendonly and logs exist, else snapshot; `loading:1` in INFO during
  restore. Replay feeds argv through `Engine.Execute` with a synthetic
  authed ConnState per record db. Shutdown: listeners → `persist.Close`
  (final fsync; shutdown snapshot when save rules configured, appendonly
  off, unsaved writes) → `shards.Close`. CONFIG gained `appendfsync`
  (live), `dir`/`dbfilename` (protected in 7.2.7 — byte-exact SET
  error; default dbfilename `dump.rdb` for GET parity); `appendonly`
  and `save` SETs are now live (open/close AOF, reschedule auto-save).
  HTTP triggers: POST `/api/v1/save`, `/bgsave`, `/bgrewriteaof`.
  Crash-recovery tests: `tests/m5_test.go` (subprocess, SIGKILL, three
  variants); unit round-trips in `lib/persist/persist_test.go`.

## M5d architecture notes

- **Soak harness** (`bin/bench-m5.sh`, `make bench-m5`, chained from
  `bin/bench.sh` unless `BENCH_M5=0`): all 8 eviction policies on Ultima
  and redis-server, `SET key:__rand_int__` over a ~20x oversubscribed
  random keyspace; gates on bounded `used_memory` (5% slack for
  cross-connection in-flight skew), advancing `evicted_keys`, and the
  byte-exact OOM/OK probe. Report: `docs/benchmarks/M5-<date>.md`.
  Gotcha: `redis-benchmark` EXITS on the first error reply (7.2.7), so
  one stray OOM fails a whole run, and no throughput summary is printed
  for runs with errors.
- **Bugs the M5d soak smoked out** (all fixed, regression tests in
  `lib/shard/evict_test.go` + pluto): (1) FlushDB left the eviction
  trackers and expiry heap fully stale → post-FLUSHALL eviction stall →
  OOM storm; (2) pluto `sharded_hash_ts` bucket placement masked the raw
  hash's low bits, and CRC-64/ISO holds its low ~28 bits constant on
  sequential decimal keys (`key:000000123456`) — whole stripes collapsed
  into one bucket chain, starving `SampleStripe` eviction sampling;
  bucket indexing now mixes via murmur3 fmix64 (`mix64`), and
  SampleStripe's attempt budget scales with stripe sparsity.

## M6 architecture notes

Detail in `docs/m6-detailed-plan.md`.

- **M6a auth core**: `lib/auth` (bcrypt accounts + refresh families,
  Ed25519 EdDSA JWTs per D20, TOTP via htotp), enforced on
  `/api/v1/*` (chi middleware), gRPC (interceptors), and WS upgrade
  (`?access_token=` / bearer subprotocol); everything gated by
  `auth.enabled` (off = pre-M6 behavior). `ConnState.User` carries the
  account on the JWT surfaces.
- **M6b resumable WS sessions** (`lib/wssession`, §9.4, D18): the
  session's `Deliver` func is the stable broker-facing funnel — stamping
  (`push_seq`), buffering, and live enqueue under one lock, so
  subscriptions survive reconnects with no re-registration and no
  reorder. `Session.Attach` is atomic: gap check → takeover close →
  handshake-OK head → replay → install sink → hand off aborted seqs.
  Sessioned WS teardown detaches (ConnState + broker subs retained) with
  a retention timer (`auth.ws_replay_buffer_ms`); expiry calls
  `eng.CloseConn`. Command replies are never replayed — lost ones
  (queued/refused/write-failed at drop) come back as `ABORTED` error
  frames by seq. Sessions bind to the creating account; a foreign resume
  is SESSION_EXPIRED. Sessionless WS connections behave exactly as M4.
- **M6c HTTP management API** (`lib/httpapi`, §10.1, D7/D11):
  `api/openapi.yaml` is the contract; `lib/httpapi` implements the
  oapi-codegen chi-server bindings (`gen/httpapi`). Handlers run engine
  commands through `Execute` on synthetic, unregistered ConnStates
  (`syntheticConn` — never `NewConnState`, which would list them as
  clients). `JsonBody` = decode (empty body lenient) →
  `config.SetDefaults` → validator/v10 over the generated `validate:`
  tags. Auth replies keep the M6a sentinel→status mapping byte-identical.
  The embedded spec copy (`lib/httpapi/openapi.yaml`, synced by
  `bin/gen-api.sh`) is served at `/api/openapi.yaml` + Swagger UI at
  `/api/docs` (both public; chi v5.3 Mount does not strip prefixes, so
  swagger.go uses http.StripPrefix); a doc-drift test pins the copies.
  `/metrics` uses a dedicated `prometheus.Registry` (PromMiddleware +
  scrape-time engine collector) guarded by the `server.metrics_allow`
  CIDR list (parsed in cmd; empty = loopback-only) — JWT-exempt.
  Middleware: request-id → logging → Recoverer → Timeout(60s) →
  Prometheus → path-aware JWT gate (admin prefix → RequireAdmin);
  chi's RealIP is deliberately omitted (deprecated, IP spoofing — and it
  would defeat the metrics allowlist). `/ws/v1` stays outside the
  Timeout/Prometheus group to preserve Unwrap/Hijack.

## M8 architecture notes

Detail in `docs/m8-detailed-plan.md`; the probing ground truth is
`docs/Redis-Errors.md`.

- **Scripting** (`lib/scripting`, decisions S1–S10 of the gopher-lua
  integration guide): wraps `gopher-lua/host` (wazero + the SHA-pinned
  `lua51_prod.wasm` blob; scripts compile to wasm). VMs come from the M8e
  R3 per-script pool (`pool.go`; S4 amended — checkout BEFORE the pause;
  a miss pays the ~44 ms instantiation, a hit is ~16-31 µs), run
  under `PauseAll` like EXEC (S3 — reused, not re-taken, when EVAL runs
  inside EXEC since txMu is not reentrant). Effects-only AOF: EVAL is
  never logged; `redis.call` inner commands ride
  `commands.runScriptCommand` (the cmdExec inner path + S9 gates:
  noscript/arity/read-only/OOM) with per-command capture and the
  `[db lua]` monitor form. Scripts compile under chunk name
  `user_script` so error texts render byte-exact, with the
  ` script: <sha>, on @user_script:N.` suffix (N from gopher-lua's
  `rt_err_line`). `redis.call` raises a plain error string (Redis's
  metatagged error-object class — recognized textually,
  `isErrorCodePrefixed`); `redis.pcall` returns
  `{err, ignore_error_stats_update=1}`; `redis.setresp` switches the
  RESP3 wrapper conversions (`{map=}/{set=}/{double=}`). BUSY gate in
  `Execute` after the soft `lua-time-limit` (only SCRIPT KILL and the
  allow_busy commands pass); SCRIPT KILL trips the VM deadline flag
  (`host.VM.Kill`) unless the script already wrote (UNKILLABLE); the
  hard `script_hard_deadline_ms` watchdog kills even writers (S5
  divergence: partial effects persist). Host-seeded RNG (S6) and
  host-side number formatting (S7). M8d: the Redis script-environment
  lockdown is byte-exact — the deps/lua readonly-table patch is ported
  into the gopher-lua runtime (Table.readonly checked in
  `luaV_settable`/`lua_rawset`/`lua_rawseti`) plus the `_G` `__index`
  error metatable (`rt_protect_globals`; KEYS/ARGV stage inside a
  `rt_globals_readonly` off-window, like Redis); enabled via
  `host.WithGlobalsProtection` (docs/Redis-Errors.md §6a). Ledgered
  divergences:
  `docs/Redis-Errors.md` §11 (no shared globals across EVALs, dialect
  texts, SCRIPT DEBUG refusal, wazero interpreter speed).
- **VM pool** (`lib/scripting/pool.go`, M8e R3): per-SHA idle queues of
  bound VMs (the host one-script law, `host.ErrScriptBound`, binds by
  `*host.Script` pointer — `Manager.Compile` and `host.Engine.Compile`
  both dedup concurrent compiles of one source to one pointer; the
  check-then-act race that broke this was found by
  TestM8ConcurrentEval). Guest GC is permanently stopped (host v1 law),
  so pooled VMs are recycled on: run count (`script_vm_recycle_runs`,
  default 100), heap watermark (`script_vm_recycle_pct`, default 75% of
  `script_max_memory_mb`, via `host.VM.UsedBytes`), fatal run errors
  (`IsVMFatal`: kill / hard deadline / "not enough memory" / trap),
  stale epoch (SCRIPT FLUSH mid-run), and pool shutdown. Depth per SHA
  is `script_vm_pool_size` (default 1 — PauseAll serializes scripts; 0
  disables pooling → the pre-M8e fresh-VM path), total idle cap
  `script_vm_pool_max` (default 64, LRU eviction). CONFIG exposes the
  four knobs read-only; INFO script gains
  `script_pool_vms/hits/misses/recycles/evictions`. Behavior is
  byte-exact by construction (globals lockdown + per-run staging/reseed)
  and the differential harness runs with the pool on.
- **gopher-lua additions consumed**: `rt_err_line` export, host
  `ScriptError.Line`, `VM.Kill`, `Engine.RegisterValue`,
  `host.ValueError` (non-string raised values); M8d:
  `rt_protect_globals`/`rt_globals_readonly` exports + the readonly
  runtime patch, `host.WithGlobalsProtection`; M8e: `VM.UsedBytes`
  (wraps the blob's `rt_mem_used_bytes`) + the `Engine.Compile`
  concurrent pointer-identity fix. Blob SHA pin
  `b4b7d2b7…` (`host/blob.go`, `testdiff/m6c_test.go`).
- **Flush fan-out tokens**: `shard.FlushDBTok/FlushAllTok/DBSizeTok` —
  FLUSHDB/FLUSHALL/DBSIZE fan out shard tasks and must carry the
  caller's pause token under PauseAll (a latent MULTI+FLUSHALL+EXEC
  deadlock smoked out by `redis.call('flushall')`).
