# AGENTS.md

Guidance for AI coding agents working in this repository. Assumes no prior
knowledge of the project.

## Project Overview

**Ultima** is a superset clone of Redis written in Go: a drop-in replacement
for the Redis wire protocol and command set, plus additional binary access
protocols (gRPC and WebSocket/protobuf) and, later, a web management UI. The
driving goal is **substantially higher throughput than Redis** by replacing
Redis's single-threaded command execution with a parallel, goroutine-based
architecture.

The authoritative reference is the design document `docs/ULTIMA-DESIGN.md`
(sections are cited throughout the code as `§N.N`, e.g. `design doc §5.2`).
Follow it — it records settled decisions (§15, D1–D19) that must not be
silently reversed. Milestones M0–M9 are defined in §14.4.

**Current status**: M0 (skeleton, three listeners), M1 (shard engine,
RESP front-end, P0 commands), M2 (P1 collections: hash/list/set/zset,
differential-green on H/L/S/Z) and M3 (P2: MULTI/EXEC/WATCH transactions,
classic pub/sub, blocking list/zset ops; differential-green) are
implemented and committed. M4 (binary front-ends) is **done**: the gRPC
front-end (typed Command envelope, Exec bidi stream, ExecBatch,
ExecGeneric, Subscribe pub/sub push stream, Monitor stream fed by
`Engine.AddMonitor`) and the WebSocket front-end (`lib/wssrv`, binary
protobuf frames at `/ws/v1`, pub/sub pushes as unsolicited seq-0 frames)
both ride the shared `lib/envelope` bridge. Exit criteria met: Go client
round-trip (tests use the generated gRPC client), TypeScript round-trip
(`tests/ts-roundtrip`, protobuf-es over `/ws/v1`, strict `tsc` typecheck),
and a RESP↔gRPC wire-byte parity gate (`tests/m4_parity_test.go`). The
parity gate also smoked out a P0 gap closed in M4: INCRBYFLOAT. M5 is
under way per `docs/m5-detailed-plan.md`: **M5a is done** — keyspace
notifications (`notify-keyspace-events`, K/E classes on
`__keyspace@<db>__`/`__keyevent@<db>__` channels over the M3 broker,
differential-green incl. expiry events) and expiry hardening (time-boxed
adaptive sweep). **M5b is done** — maxmemory eviction (all 8 Redis
policies, per-shard memory accounting and eviction loops, the OOM gate
with byte-exact `OOM`/`EXECABORT` replies; differential-green on the
gate strings). **M5c is done** — persistence (`lib/persist`: own-format
snapshot + per-shard AOF with global sequence merge at replay,
SAVE/BGSAVE/LASTSAVE/BGREWRITEAOF, restore-before-serve, crash-recovery
tested; M5c also closed the P0 gap EXPIREAT/PEXPIREAT, needed by the
AOF's PEXPIREAT rewrite). **M5d is done** — the maxmemory soak
(`bin/bench-m5.sh`, `make bench-m5`): sustained SET load at 20x keyspace
oversubscription against a bounded maxmemory under all 8 eviction
policies, gating on bounded `used_memory`, advancing `evicted_keys`, and
the byte-exact OOM/probe behavior; report in `docs/benchmarks/M5-<date>.md`;
memo in `note/M5-implemented.md`. Later
milestones from the
design layout
(§14.1: `lib/persist`, `clients/`, `web/`, `api/`, extra CLIs under
`cmd/`) do **not** exist yet.

## Technology Stack

- **Go 1.27** (module `github.com/pschlump/ultima`).
- Key dependencies (see `go.mod`):
  - `github.com/pschlump/pluto` — generic data-structure library (thread-safe
    `_ts` variants). **Pulled in via `replace` directive to the sibling
    checkout `../pluto`** — that directory must exist for the module to
    build. Its per-structure specs live in `docs/pluto/`. **Standing
    permission**: when the efficient way to implement something is to add a
    feature to `../pluto` (e.g. `lru_ts`, `lfu_ts`), add it to the data-structure
    library — with tests there — rather than working around its API here.
    Update the matching `docs/pluto/` spec when you do.
  - `github.com/go-chi/chi/v5` — HTTP router for the management surface.
  - `github.com/gorilla/websocket` — WebSocket endpoint.
  - `google.golang.org/grpc` + `protobuf` — gRPC front-end.
  - `go.uber.org/goleak` — goroutine-leak detection in tests.
- **No CGo**, no Docker/CI config currently in the repo.
- `note/redis/` (gitignored) holds a Redis source checkout used as a
  reference; `note/` is scratch material, excluded from lint.

## Runtime Architecture

One binary (`ultima-server`), one process, **three network surfaces**
(design doc §3), all wired in `cmd/ultima-server/startup.go`:

| Surface        | Default addr | Implementation                          |
|----------------|--------------|------------------------------------------|
| RESP (Redis protocol) | `:6379` | `lib/resp` (vendored redcon fork) → `lib/commands.Engine` |
| gRPC           | `:6380`      | `lib/grpcsrv` on generated `gen/go/ultima/v1` code; server reflection on |
| HTTP/WebSocket | `:6381`      | `lib/handler` + `lib/wssrv` mounted on a chi mux (`cmd/ultima-server/router.go`) |

Request flow: each front-end parses its wire format, calls
`commands.Engine.Execute(ConnState, args)`, and renders the returned
`resp.Value` (decision D3: one command engine, three front-ends).

- **Sharding** (`lib/shard`): the keyspace of each logical DB lives in one
  pluto `sharded_hash_ts.ShardedHash` whose stripe count equals the shard
  count; key → shard routing is Fibonacci hashing,
  `(crc64(key)*0x9E3779B97F4A7C15) >> (64-log2 N)` — identical to the
  table's internal stripe routing, so stripe i is shard i (raw CRC bits
  cluster for short/structured keys; do not "simplify" this). Each shard
  has an owner goroutine draining a task queue (`Engine.Do` / `DoMulti` /
  `DoShard`); all data-mutating work runs inside the owning shard's
  goroutine — no caller-side locks on the hot path. Multi-shard fan-out
  closures (`DoMulti`) run concurrently — shared writes must be
  synchronized or slot-indexed. Shard-count helpers
  only run inside the shard goroutine (see the comment block at
  `lib/shard/shard.go:380`).
- **Expiry**: passive on access plus an exact per-shard min-heap
  (`heap_ts`) swept periodically by the shard goroutine (D5).
- Shard count: config `shard_count`, `0` = 4×GOMAXPROCS, rounded up to a
  power of two (`shard.ResolveShardCount`).
- Graceful shutdown: SIGINT/SIGTERM → drain gRPC, close HTTP and RESP,
  stop shard goroutines (10 s cap).

**Commands implemented so far**: M1/P0 — connection (PING, ECHO, HELLO,
AUTH, SELECT, QUIT), strings (SET/GET family, INCR/DECR family +
INCRBYFLOAT (M4), APPEND,
STRLEN, MGET/MSET/MSETNX), keyspace (DEL, EXISTS, EXPIRE/PEXPIRE/
EXPIREAT/PEXPIREAT (M5c), TTL/PTTL,
PERSIST, TYPE, SCAN), server (INFO, DBSIZE, FLUSHDB/FLUSHALL, CONFIG,
CLIENT, COMMAND, SAVE/BGSAVE/LASTSAVE/BGREWRITEAOF (M5c)). M2/P1 — hashes (HSET/HGET/HMSET/HMGET/HGETALL/HDEL/
HEXISTS/HLEN/HKEYS/HVALS/HINCRBY/HINCRBYFLOAT/HSETNX/HSTRLEN/HRANDFIELD/
HSCAN), lists (LPUSH/RPUSH/LPUSHX/RPUSHX/LPOP/RPOP/LLEN/LRANGE/LINDEX/
LSET/LINSERT/LREM/LTRIM/RPOPLPUSH/LPOS/LMOVE), sets (SADD/SREM/SMEMBERS/
SISMEMBER/SMISMEMBER/SCARD/SPOP/SRANDMEMBER/SMOVE/SINTER/SUNION/SDIFF/
SINTERSTORE/SUNIONSTORE/SDIFFSTORE/SINTERCARD/SSCAN) and sorted sets
(ZADD/ZSCORE/ZMSCORE/ZINCRBY/ZRANK/ZREVRANK/ZRANGE/ZRANGEBYSCORE/
ZRANGEBYLEX/ZREVRANGE/ZREVRANGEBYSCORE/ZREMRANGEBYRANK/ZREMRANGEBYSCORE/
ZREMRANGEBYLEX/ZCARD/ZCOUNT/ZLEXCOUNT/ZREM/ZPOPMIN/ZPOPMAX/ZRANDMEMBER/
ZDIFF/ZINTER/ZUNION/ZINTERSTORE/ZUNIONSTORE/ZSCAN). M3/P2 — transactions
(MULTI/EXEC/DISCARD/WATCH/UNWATCH, RESET), pub/sub (SUBSCRIBE/UNSUBSCRIBE/
PSUBSCRIBE/PUNSUBSCRIBE/PUBLISH/PUBSUB; SSUBSCRIBE deferred to M8) and
blocking ops (BLPOP/BRPOP/BLMPOP/
BLMOVE/BRPOPLPUSH, BZPOPMIN/BZPOPMAX/BZMPOP). M5a adds
`notify-keyspace-events` keyspace notifications (CONFIG key, off by
default). Value types: strings
plus the four collections (`lib/types`). Ultima reports Redis compatibility
version 7.2.7 (`commands.CompatVersion`).

M3 architecture notes:

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

M5a architecture notes:

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
  deletions reach it via `Shard.OnKeyGone` (reason `expired`; M5b will
  reuse it for `evicted`). Known divergence: `SET k v PXAT <past>` —
  Ultima deletes inline at command time (no later lazy `expired` event;
  replies match Redis).
- **Expiry hardening**: `Shard.sweep` is time-boxed (`SweepTimeBox`, 1 ms)
  in addition to the `SweepMax` pop cap, and the cadence is adaptive —
  halving toward `SweepFloor` (10 ms) while sweeps stop with due work
  left, relaxing back to `SweepInterval` (100 ms) when clean
  (`nextSweepInterval`; the `active_expire_effort` analogue, §5.2).

M5b architecture notes:

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
  sampling (new pluto `sharded_hash_ts.SampleStripe`, per-stripe since
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

M5d architecture notes:

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

M5c architecture notes:

- **Snapshot** (`lib/persist/snapshot.go`, own format per D9): magic
  `ULTIMA01`, header, one segment per (db, shard) with CRC-64/XZ (pluto
  `crc`) per segment + whole-file trailer; payloads optionally
  LZW-compressed (pluto `quicklist.LZWCodec()`, `snapshot_compress`).
  Each segment is serialized inside its shard goroutine via
  `Shard.DumpDB` (walks only stripe i through the new pluto
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

## Code Organization

```
cmd/ultima-server/   main binary: main.go, startup.go (wiring), router.go (chi), version.go (build-stamp vars)
lib/config/          JSON config: `default:"..."` struct tags via reflection + `$ENV$NAME` env substitution (D6)
lib/resp/            vendored + extended fork of tidwall/redcon v1.6.4 (D2); adds RESP3 emitters,
                     per-connection protocol versioning; kept close to upstream — excluded from lint
lib/respserver/      shared RESP front-end wiring (§6.1, D3): builds the *resp.Server with the
                     accept/handler/closed closures bridging to commands.Engine; used by
                     cmd/ultima-server and both test harnesses
lib/shard/           sharded keyspace engine, owner goroutines, routing, expiry heap,
                     WATCH dirty tracking (tombstones/epochs), EXEC pause (PauseAll),
                     blocking-waiter FIFO registry; evict.go (M5b maxmemory: per-shard
                     accounting, LRU/LFU/ttl/random eviction loops, policy enum)
lib/types/           collection value types in Entry.Obj (M2): Hash (insertion-ordered
                     slice → slice+map past hash-max-listpack-*), List (pluto
                     quicklist_ts wrapped with a byte counter, §5.3 #10), Set (sorted
                     int64 slice intset → map past
                     set-max-intset-entries), ZSet (skip_list_ts + member→score map,
                     §5.3 #1). Promotion is one-way, like Redis. memusage.go (M5b):
                     O(1) per-type memory estimators fed by tracked content bytes.
lib/pubsub/          classic pub/sub broker (M3): channel/pattern subscription maps,
                     delivery on the publisher's goroutine into per-conn push queues
lib/commands/        front-end-agnostic command engine; table.go is the command registry
                     (def(name, arity, flags, first, last, step, group, handler));
                     hash.go/list.go/set.go/zset.go hold the P1 handlers, coll.go the
                     shared parsing helpers (string2d-exact floats, range bounds);
                     tx.go (M3 MULTI/EXEC/WATCH), pubsub.go (M3 subscriptions + gate),
                     block.go (M3 blocking ops, park/wake engine),
                     notify.go (M5a notify-keyspace-events: class parser,
                     notifyKeyspace emission point), aof.go (M5c Persister
                     interface + capture/rewrite rules), persist.go (M5c
                     SAVE/BGSAVE/LASTSAVE/BGREWRITEAOF)
lib/envelope/        shared bridge (M4): protobuf Command → engine argv, resp.Value ↔
                     protobuf Value (RESP3 mirror; FromProto is the inverse, used by
                     the RESP↔gRPC wire-byte parity gate); used by grpcsrv and wssrv (D3/D15)
lib/grpcsrv/         gRPC front-end (M4: Exec bidi stream, ExecBatch, ExecGeneric, Ping,
                     Subscribe pub/sub push stream, Monitor stream over Engine.AddMonitor)
lib/wssrv/           WebSocket front-end (M4, §6.3): binary protobuf Command frames at
                     /ws/v1, one frame per command, seq-correlated replies; pub/sub
                     pushes as unsolicited seq-0 frames over a bounded queue (slow
                     consumer → close, as in lib/respserver)
lib/handler/         HTTP routes (/health, /ready, /api/v1/ping, POST
                     /api/v1/save|bgsave|bgrewriteaof) + RequestLogger
lib/persist/         persistence (M5c, §13.1, D9 own formats): format.go
                     (snapshot codec, CRC-64/XZ, LZW payloads), snapshot.go
                     (per-(db,shard) segments via Shard.DumpDB; WriteSnapshot[Tok]
                     + LoadSnapshot), aof.go (per-shard seq-stamped logs +
                     BGREWRITEAOF dump), replay.go (seq merge + broadcast
                     dedup), manager.go (save rules, fsync policies,
                     restore-before-serve, INFO persistence fields)
proto/ultima/v1/     protobuf IDL
gen/go/ultima/v1/    generated protobuf Go bindings (do not hand-edit)
gen/ts/ultima/v1/    generated protobuf TypeScript bindings (protobuf-es; do not hand-edit)
tests/               integration_test.go (three surfaces, ephemeral ports), m3_test.go
                     (M3 real-socket pub/sub + blocking + WATCH/EXEC stress tests),
                     m4_grpc_test.go / m4_ws_test.go (M4 front-ends), m4_parity_test.go
                     (RESP↔gRPC wire-byte diff), m4_ts_test.go (TS round-trip driver),
                     m5_test.go (M5c crash recovery: subprocess + SIGKILL,
                     snapshot/AOF/AOF-rewrite variants)
tests/ts-roundtrip/  protobuf-es TS client script (bun; `bun install` first) — the M4
                     go+ts round-trip exit criterion; strict tsc typecheck via tsconfig
tests/differential/  harness diffs replies against a real redis-server (the parity gate);
                     multi-connection scripts, push frames and blocking wakeups supported
bin/                 gen.sh (protoc), gen-build-stamp.sh (ldflags), bench.sh (M1 sweep,
                     chains into bench-pubsub.sh for the M3 pub/sub benchmark and
                     bench-m5.sh for the M5 maxmemory soak)
docs/                ULTIMA-DESIGN.md, pluto/ structure specs, benchmarks/ reports
note/                scratch/reference (Redis checkout, benchmarks); gitignored, lint-excluded
```

Adding a new command: implement a handler in the appropriate
`lib/commands/*.go` file, register it in `table.go`'s `init()`, and carry
**Redis-exact semantics and error strings** (see `engine.go` helpers like
`parseIntStrict`, `errUnknownCommand` for the byte-exact conventions).

## Build and Test Commands

All via the Makefile (default goal is `build`):

- `make build` — builds `./ultima-server` with git/build-stamp ldflags
  (`bin/gen-build-stamp.sh` → `-X main.Version=` etc.; `--version` prints it).
- `make run` — builds and runs with `ultima.cfg.json` (reads password from
  `$ENV$ultima_password`).
- `make test` — `go test ./...` (unit + integration + differential).
- `make lint` — `golangci-lint run` (v2 config in `.golangci.yml`).
- `make gen_proto` — regenerate protobuf bindings from `proto/` into
  `gen/go`; requires `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc`.
  Also emits `gen/ts` (protobuf-es) via `bin/gen-ts.sh`, which no-ops with
  a hint if the TS plugin isn't installed (`bun install` in
  `tests/ts-roundtrip` provides it).
- `make bench` — `bin/bench.sh`: Ultima vs local `redis-server` via
  `redis-benchmark`; writes a report to `docs/benchmarks/M1-<date>.md`,
  then runs `bin/bench-pubsub.sh` (M3 pub/sub sweep, subscribers are the
  `note/pubsub-bench-sub` Go driver; skip with `BENCH_PUBSUB=0`), which
  writes `docs/benchmarks/M3-<date>.md`, then `bin/bench-m5.sh` (M5
  maxmemory soak over all 8 eviction policies, gated on bounded memory;
  skip with `BENCH_M5=0`), which writes `docs/benchmarks/M5-<date>.md`.
  Tunable via `REDIS_BIN`, `BENCH_BIN`, `BENCH_*_PORT`, `BENCH_REQUESTS`,
  `BENCH_SUBS`, `BENCH_SINGLE_REQUESTS`; the soak has its own
  `BENCH_M5_*` knobs (requests, maxmemory MB, datasize, keyspace, TTL).
- `make bench-m5` — just the M5 maxmemory soak.
- `make tidy`, `make clean`.

Configuration: JSON file (`ultima.cfg.json` by default). Defaults come
from struct tags, then the file overrides; `$ENV$NAME` tokens in string
values are expanded from the environment (`lib/config/config.go`).

## Testing Instructions

1. **Unit tests** colocated with code (`lib/**/*_test.go`); goleak guards
   against goroutine leaks in engine tests.
2. **Integration tests** (`tests/integration_test.go`): boot all three
   listener surfaces on `127.0.0.1:0` and exercise them with real clients;
   RESP wiring comes from the shared `lib/respserver` package (D3).
   `tests/m3_test.go` adds real-socket pub/sub, blocking, and WATCH/EXEC
   stress tests (M3 exit criterion).
3. **Differential tests** (`tests/differential/`): scripted command
   sequences run against Ultima (in-process, ephemeral port) and a real
   `redis-server` subprocess; decoded replies **including error strings**
   are diffed — this is the primary parity gate. Requires `redis-server`
   on PATH (or `REDIS_BIN`); skipped under `-short` or `DIFFERENTIAL=0`.
   **Standing permission**: `redis-server` and `redis-cli` may be run on
   this machine at any time for probing behavior (there is no data on the
   local Redis that can be broken). Prefer probing the live installed
   server (7.2.7, the compat target) over reading `note/redis/`, which is
   a newer 8.x source checkout and can diverge from 7.2.7 behavior.
   Scripts live in `scripts.go` (P0), `scripts_p1.go` (P1), `scripts_m3.go`
   (P2: transactions, pub/sub, blocking — uses the multi-connection
   `cmdOn`/`sendOn`/`recvOn`/`expectPush` step constructors documented in
   `compare.go`), `scripts_m5.go` (M5a: keyspace notifications — notify
   scripts must reset `notify-keyspace-events ""` at the end; config
   persists across scripts; M5b: OOM gate + policy CONFIG; M5c:
   SAVE/BGSAVE/LASTSAVE/BGREWRITEAOF reply shapes + persist CONFIG keys —
   the harness's Ultima gets a temp-dir persist manager and its redis a
   temp `--dir`); extend the appropriate file when adding commands.

Running `go test ./...` also compiles `note/grpc-vs-text-benchmark` and
`note/crc-probe` (scratch modules kept for reference).

## Code Style Guidelines

- Standard Go; `gofmt`/`goimports` enforced via golangci-lint formatters.
- Enabled linters: errcheck, govet, ineffassign, staticcheck, unused,
  misspell, revive.
- Lint/format **exclusions**: `gen/`, `note/`, `docs/`, and `lib/resp/`
  (vendored redcon fork — keep it close to upstream; do not restyle it).
- Package doc comments reference design-doc sections (`design doc §N.N`)
  and decision numbers (D1–D19); keep that convention, and update or add
  references when implementing a documented section.
- Comments explain the *why* (Redis-semantics notes, invariants such as
  "call only from inside the shard goroutine"). Byte-exact Redis error
  messages are a hard requirement, validated by the differential harness.
- Never hand-edit files under `gen/`; change `proto/` and run
  `make gen_proto`.

## Security Considerations

- `requirepass` auth: when set, the command engine gates every command
  except AUTH/HELLO/QUIT behind `NOAUTH` (`lib/commands/engine.go`).
- `Config.CheckStartupPosture` (`lib/config/config.go:142`) warns when the
  RESP listener binds all interfaces with no `requirepass` and no TLS.
- Config secrets are injected via `$ENV$NAME` substitution — do not commit
  real credentials to config files.
- Command renaming (Redis's `rename-command`) is deliberately **excluded**
  as security by obscurity (design doc §1.2); protection comes from auth
  and, later, ACLs.
- The M4 WebSocket endpoint (`lib/wssrv`) has no origin policy
  (`CheckOrigin: true`) and no upgrade-time auth yet; real auth (JWT +
  TOTP 2FA via `pschlump/htotp`) is scheduled for M6 (§9).
  `note/redis-security-overview.md` is the security reference.
- Command execution must never panic on client input; all errors are reply
  values (`Engine.Execute` contract).
