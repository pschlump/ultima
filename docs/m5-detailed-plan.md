# M5 detailed plan — Expiry hardening, eviction, persistence, keyspace notifications

Design doc §14.4 M5: "Expiry hardening, eviction, persistence (snapshot + AOF),
keyspace notifications (notify-keyspace-events; deferred from P2)". Exit
criteria: **crash-recovery tests; maxmemory soak**.

Relevant settled decisions: D5 (heap-based exact expiry), D9 (own snapshot/AOF
format first, RDB-compat later), D14 (exact LRU via pluto `lru_ts`). Do not
reverse these.

Execution is phased M5a → M5d, each phase independently green
(`go test ./...`, `make lint`, differential where applicable). Config/CONFIG
keys use **Redis-parity names** (`appendonly`, `appendfsync`,
`maxmemory-policy`, `notify-keyspace-events`, `save`, `dir`, `dbfilename`,
`appendfilename`) rather than the §8 sketch names (`aof_enabled`, …) — the
differential gate diffs CONFIG GET replies byte-exact, so parity names win.
§8 is a sketch, not a D-decision.

**Status: M5a done, M5b done, M5c done. M5d not started.**

---

## M5a — Keyspace notifications + expiry hardening

### Notifications

- `lib/commands/notify.go`:
  - Flag parser for `notify-keyspace-events` values (Redis class letters
    `K E g $ l s h z x e m d A`; unsupported-in-7.2.7 letters match real
    redis-server accept/reject behavior, probed live).
  - `notifyFlags atomic.Uint64` bitmask on `commands.Engine`; class-gated
    `e.notifyKeyspace(db, key, event)`.
  - Delivery via the existing broker: `__keyevent@<db>__:<event>` (payload =
    key) and `__keyspace@<db>__:<key>` (payload = event). Broker is a field on
    Engine; Publish is mutex-guarded, non-blocking, safe from shard
    goroutines; gRPC/WS push paths are frame-generic.
- Emission sites: every handler mutation ends in
  `Shard.Store`/`Touch`/`Delete`/`PushExpire` (~40 sites across
  `lib/commands/{strings,hash,list,set,zset,keys,block}.go`); annotate each
  with Redis-exact event names mapped from `note/redis/src/notify.c`.
- Expiry-driven events: shard-level sink `Shard.OnKeyGone func(db, key,
  reason)` set by `commands.Engine`; invoked from passive expiry in
  `Shard.Lookup` and the active sweep with reason `expired` (M5b adds
  `evicted`).
- CONFIG: `notify-keyspace-events` in `configParams`; `notify_keyspace_events`
  in `ServerConfig`; wired in `startup.go`.

### Expiry hardening

- Time-box `Shard.sweep`: per-tick deadline in addition to the SweepMax pop
  cap.
- Adaptive cadence: shorten the sweep interval toward a floor while sweeps
  keep finding due keys, relax when clean (Redis `active_expire_effort`
  analogue).
- Restore path (M5c) must rebuild `PushExpire` heap entries.

### Tests

- `tests/differential/scripts_m5.go`: CONFIG SET parity, `__keyevent@0__:*`
  push frames via `expectPush`, expiry events.
- `lib/commands/notify_test.go` flag-parser table tests; sweep time-box unit
  test in `lib/shard/shard_test.go`.

---

## M5b — maxmemory eviction

**Done.** As below, with these as-built adjustments: the LRU/LFU
trackers (per-shard, keyed by `(db,key)`, always-on like Redis's
per-object clock) needed two pluto additions — `lru_ts`/`lru`
`Oldest`/`PopOldest` (the `Backward()` iterator snapshot is O(n), too
slow per victim) and `sharded_hash_ts.SampleStripe` (per-stripe random
sampling for LFU candidates and the random policies). `Shard.Touch`
gained `(db, key)` to feed accounting. The OOM gate replicates three
probed 7.2.7 behaviors: queue-time OOM rejects ANY queued command while
over limit, EXEC of a denyoom-containing tx aborts with the OOM reason
embedded, and eviction policies get a synchronous `EvictNow`
(performEvictions analogue) before rejection. The eviction-reprieve case
is not differential-scriptable (Redis counts process baseline, Ultima
counts keyspace, D10) — unit-tested instead.

### Memory accounting (prerequisite — nothing exists today)

- Add `MemUsage() int64` O(1) estimators per type in `lib/types` (hash:
  count×overhead + tracked pair bytes; list: quicklist segment metadata;
  set: count or intset bytes; zset: count×(skiplist node + map overhead);
  string: len). Estimates, documented as such (D10 already trades exactness).
- `shard.Entry` gains cached `memBytes int64`; `Shard` gains a `usedBytes`
  counter. Update inside the existing write choke points: `Store` (add new −
  cached old), `Delete`/expiry (subtract), `Touch` (recompute and adjust).
  All inside the shard goroutine — no locks.
- INFO `used_memory` switches to the keyspace counter (keep Go-heap value as
  `used_memory_process`); INFO `maxmemory_policy` reflects the real policy
  (`server.go:208` currently hardcodes `noeviction`).

### Eviction engine (`lib/shard/evict.go`)

- Per-shard LRU: pluto `lru_ts` (unbounded capacity; victims popped from
  `Backward()`); touch on read in `Shard.Lookup`, on write in
  `Store`/`Touch`; delete on `Delete`/expiry.
- Per-shard LFU: pluto `lfu_ts` (`NewLfu[string](logFactor, decayMinutes)`)
  for `allkeys-lfu`/`volatile-lfu`; same touch sites.
- volatile-* policies: volatile-only LRU/LFU instances holding only keys
  with TTL (or `NewLruFunc` veto); `volatile-ttl` pops the existing expiry
  heap (shortest TTL first = heap order).
- Random policies need a random key from a stripe: check `sharded_hash_ts`
  for a `Random()` API; if absent, random-start scan cursor (bounded steps)
  via a new `Engine.RandomKey(db)` helper.
- Eviction loop: after each write task in the shard goroutine, if
  `maxmemory>0 && shardUsed > maxmemory/shardCount`, evict per policy until
  under quota or no evictable keys. Policy from an atomic on shard.Engine
  set by CONFIG. Emit `evicted` keyspace event (via the M5a `OnKeyGone`
  sink) + `evicted_keys` stat. M5c adds synthesized AOF `DEL` records from
  the same sink.
- `noeviction` + over limit: write commands flagged `denyoom` (flag already
  exists in table.go, currently inert) return Redis's exact OOM error
  (probe real redis-server for the string).

### CONFIG / config file

- `maxmemory-policy` (enum, all 8 Redis policies) in `configParams` → atomic
  on `commands.Engine` → propagated to shard.Engine. `maxmemory` already
  plumbed; eviction consumes it.
- `lib/config/config.go`: `maxmemory_policy` (default `noeviction`) in
  `ServerConfig`; wired in `startup.go` beside `SetMaxMemory`.

### Tests

- Unit tests per policy (fill to limit, assert victim choice + OOM error);
  differential script for OOM error string. Absolute memory numbers are NOT
  diffed against Redis (Go encodings, D10) — assert policy names, OOM
  behavior, relative ordering only.

---

## M5c — persistence (`lib/persist`, new package)

Own format per D9. No new module dependencies: CRC-64 via pluto `crc`,
compression via pluto `quicklist.LZWCodec()` (the documented LZF stand-in)
behind a `compress` config flag.

**As-built adjustments (from implementation + live-7.2.7 probes):**

- `sharded_hash_ts.StripeWalk(i)` was added to pluto (per-stripe walk;
  the per-shard dump walks only its own stripe).
- The EXEC gap resolution changed: 7.2.7's AOF does NOT wrap transactions
  in MULTI/EXEC (probed — replicas get the framing, the AOF doesn't).
  cmdExec captures each queued command individually instead.
- Rewrite rules probed against live 7.2.7 AOF bytes: EXPIRE-family →
  `PEXPIREAT key abs-ms [cond]` (condition kept!); SET … EX/PX/EXAT →
  `PXAT abs`; GETEX … EX/PX/EXAT → PEXPIREAT, … PERSIST → PERSIST;
  BLPOP/BRPOP → LPOP/RPOP of the replied key (even when it never
  blocked); BLMOVE → LMOVE sans timeout; BRPOPLPUSH → RPOPLPUSH;
  BZPOPMIN/MAX → ZPOPMIN/MAX; BLMPOP/BZMPOP → single-key LMPOP/ZMPOP;
  SPOP → SREM of the replied members; timeouts/null replies log nothing.
  EXPIREAT/PEXPIREAT had to be implemented as commands (P0 gap) since
  the AOF speaks PEXPIREAT.
- Per-shard AOF records carry a global sequence number and per-record db
  (`["ULTIMAREC","<db>","<seq>",cmd,args...]`), and replay is a merge by
  seq with broadcast (FLUSHDB/FLUSHALL) dedup — a plain per-file replay
  loses cross-shard ordering (found by unit test: a broadcast FLUSHDB
  replayed once per shard file at per-shard positions clobbered other
  shards' replayed keys). BGREWRITEAOF dump records take fresh seqs from
  the same counter; post-restart seqs resume above the replayed max.
- BGREWRITEAOF takes PauseAll for the dump+swap plus a capture drain
  (`commands.Engine.PersistQuiesced`): commands caught between mutation
  and AOF append are waited out, so no mutation is both in the dump and
  in the new log (which would duplicate INCR/RPUSH effects on replay).
  Stop-the-world rewrite, proportional to keyspace size — the documented
  divergence from fork+COW; chunked dump is the follow-up.
- v1 limitation (documented): AOF record order follows per-connection
  completion order; same-key writes racing across connections can replay
  in the other relative order. Apply-time sequence assignment is the M8
  (replication) answer.
- SAVE deadlocks if the dump runs tok-0 DoShard under PauseAll — SAVE
  uses WriteSnapshotTok with the pause token (found by smoke test).
- `dir` and `dbfilename` are protected configs in 7.2.7 (byte-exact
  "can't set protected config" on SET); default dbfilename is `dump.rdb`
  for CONFIG GET parity.

### Snapshot (RDB-equivalent)

- Format (`lib/persist/format.go`): magic `ULTIMA01`, header (version, shard
  count, db count, timestamp), then per-(db,shard) segments; each entry
  record: key, type tag, expireAtMs, type-tagged payload (string bytes; hash
  pairs via `Hash.Each`; list via `quicklist_ts.All()`; set via
  `Set.Members()`; zset via a new exported `ZSet.Each(fn)` in lib/types).
  CRC-64 per segment + whole-file trailer. Manifest file
  (`snapshot.manifest` JSON: segments, checksums, save time).
- Writers run **per shard inside `DoShard` tasks** (stripe i == shard i, so
  the shard goroutine is the only writer — an in-task dump is consistent
  with no pause; §13.1 staggered design). Serialize stripe i by walking the
  table's `Walk` and bucketing by `eng.ShardIndex(key)`, or add a
  `StripeWalk(i)` helper to pluto if filtering proves wasteful. Write to
  `tmp` + rename for atomicity.
- `SAVE` = `PauseAll` + synchronous whole-server dump; `BGSAVE` = per-shard
  staggered tasks (per-shard point-in-time, documented divergence from
  fork-RDB); `LASTSAVE` from manifest/atomic; INFO persistence section
  fields (`rdb_bgsave_in_progress`, `rdb_last_save_time`, `aof_enabled`, …).
- Auto-save: `save "900 1 …"` rules already parse-validated
  (`validSaveParam`); add a checker goroutine in the persist manager (dirty
  counter vs. rules).

### AOF

- Per-shard append logs + manifest (MP-AOF-like, §13.1):
  `aof/shard-<i>.aof` + `aof.manifest`. Records = RESP-serialized commands
  (reuse lib/resp writer) — replay = feed argv back through
  `commands.Engine.Execute` with a synthetic authed ConnState per DB.
- Capture point: `commands.Engine.Execute` post-handler (`engine.go:293`),
  gated on `slices.Contains(def.Flags, "write")` and success (Redis
  dirty-counter semantics; v1: log write commands whose reply is not an
  error, with per-command exceptions if differential shows drift).
- EXEC gap: queued handlers bypass Execute (`tx.go:99` calls `qdef.Handler`
  directly) — hook `cmdExec` to emit `MULTI` + queued cmds + `EXEC` framing
  into the AOF (Redis logs the transaction as a unit).
- Blocking commands: a woken BLPOP/BRPOP/BLMOVE is rewritten as its
  non-blocking effect (LPOP/RPOP/LMOVE) — capture in `block.go` wake path
  where the popped value is known; timeouts log nothing.
- Shard-originated mutations (expiry sweep, passive expiry, eviction):
  synthesized `DEL`/`PEXPIREAT` records emitted from the `OnKeyGone` sink.
- fsync goroutine in the persist manager: `always` (per batch), `everysec`
  (1 s ticker), `no`. Started in `startup.go`, stopped in shutdown before
  `shards.Close()`.
- `BGREWRITEAOF`: dump current state as a minimal command stream
  (SET/HSET/RPUSH/SADD/ZADD + PEXPIREAT) into new per-shard logs, atomic
  swap.

### Restore + wiring

- Startup (`cmd/ultima-server/startup.go`): construct the persist manager
  after engine setup, **synchronously restore before any Serve starts**
  (move `net.Listen` after restore or gate accepts, to kill the race).
  Precedence: appendonly → AOF replay; else snapshot. Restore rebuilds the
  expiry heap via `PushExpire`; tombstone/watch state starts fresh.
  `loading:1` in INFO during restore.
- Config: new `persist` section in `lib/config` (`dir` default `./data`,
  `dbfilename`, `appendfilename`, `appendonly`, `appendfsync`, `save`
  string, `snapshot_compress` bool); same keys in `configParams` for CONFIG
  GET/SET parity (`dir`, `appendonly`, `appendfsync`; `save` set now
  actually schedules).
- HTTP triggers (§13.1): `POST /api/v1/save`, `/api/v1/bgsave`,
  `/api/v1/bgrewriteaof` in `lib/handler` + `router.go`.
- Shutdown: listeners closed → final fsync (+ snapshot if `save` rules
  demand) → `shards.Close()`.

### Tests

- `lib/persist/persist_test.go` with `goleak.VerifyTestMain`: round-trip
  encode/decode every type incl. TTLs; corrupt-CRC rejection; AOF replay
  equivalence.
- `tests/m5_test.go`: crash-recovery — spawn `./ultima-server` subprocess on
  the `startRedis` template (`tests/differential/harness.go:203-234`) with a
  temp-dir config, load data, `SIGKILL`, restart against the same `dir`,
  assert data + TTLs survive (snapshot-only, AOF-everysec, AOF-always
  variants). Plus an in-process restart harness for fast iteration.
- Differential: SAVE/BGSAVE/LASTSAVE reply shapes, CONFIG GET/SET of new
  keys.

---

## M5d — Exit criteria, benchmark, docs

- **maxmemory soak**: `bin/bench-m5.sh` — sustained write load against
  `maxmemory` with each eviction policy, assert bounded memory + throughput;
  write `docs/benchmarks/M5-<date>.md` in the M1/M3 report format.
- `note/M5-implemented.md` memo in the M3/M4 format (scope, architecture
  bullets with § refs, bugs found, verification, benchmark pointer).
- Update `AGENTS.md` (status paragraph, command list — SAVE/BGSAVE/
  BGREWRITEAOF/LASTSAVE + new CONFIG keys, `lib/persist` in the layout,
  architecture notes).
- Update `docs/ULTIMA-DESIGN.md` §8 sketch note (Redis-parity config names
  chosen) and mark the M5 row done in §14.4.
- Final gate: `make build && make test && make lint` all green.

## Risks / open points

- Exact Redis error strings for OOM and bad `notify-keyspace-events` values:
  probe real redis-server first (on PATH per the harness).
- `sharded_hash_ts` may lack a random-key API → fallback random-start scan;
  may lack per-stripe iteration → bucket-by-ShardIndex during `Walk` (or add
  `StripeWalk` to ../pluto — same author's sibling repo, already
  replace-directed).
- In-task per-shard snapshot stalls that shard's queue for large shards —
  acceptable for v1 (documented); chunking via cursor is the follow-up if
  the soak shows stalls.
- Byte-exact `used_memory` parity with Redis is impossible (D10) —
  differential scripts avoid asserting absolute memory numbers.
