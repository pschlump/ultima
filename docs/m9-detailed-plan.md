# M9 detailed plan — Superset features (§12) + performance campaign

Design doc §14.4: "**M9** | Superset features (§12) + performance campaign | ≥4×
Redis on target workload; final report". The target workload is defined by §1.2:
**GET/SET-heavy mixes over RESP, measured with redis-benchmark on 8+ cores.**

**Exit criteria:**

1. ≥4× Redis 7.2.7 throughput on the target workload (median of ≥3 runs,
   pipelined GET/SET mixes, 8+ cores), reported in `docs/benchmarks/M9-<date>.md`
   with pprof profiles attached (§14.2).
2. §12 superset features delivered, each behind a config flag: per-shard metrics
   & heatmaps (#2, finish), Batch API (#3, extend to WS), keyspace change feeds
   (#4), multi-pattern SCAN + server-side transforms (#5).
3. All existing gates stay green: `go test ./...` (incl. differential byte-exact
   suite), `make lint`, `CGO_ENABLED=0 go build ./...`, CLI matrix in both modes.

Scope decisions for this plan: §12.6 (pluggable persistence backends /
`b_tree_disk_ts` warm tier) is **deferred post-M9** (§12 marks it experimental).
The EVAL throughput gap (~0.01–0.03× Redis; wazero R3 is interpreted on
darwin/arm64 — structural, see `docs/benchmarks/M8-2026-09-23.md`) is **excluded
from the 4× gate**; the target workload is GET/SET-heavy per §1.2. Pub/sub
fan-out (0.08–0.21× with 8 subscribers) gets a **secondary target** (≥1× Redis,
no dropped slow consumers) but does not gate M9.

Execution is phased M9a → M9h, **performance first** (the current ratio is
0.40–0.45×, roughly 9× short of the gate — the milestone's dominant risk), then
features, then a final re-measurement and report (features add hot-path cost, so
the gate numbers are re-verified last). Each phase is independently green
(`go test ./...`, `make lint`).

Relevant settled decisions: D2 (vendored redcon — keep `lib/resp` close to
upstream), D3 (one command engine, three front-ends — batch/CDC feed through
`commands.Engine` and `lib/envelope`), D5 (expiry heap), D9 (own persistence
formats), D18 (§9.4 WS session recovery — CDC rides on it), D20 (auth).
**Do not silently reverse these** — in particular the owner-goroutine shard
model (§4.1/§4.3). If profiling shows the 4× gate requires changing a settled
decision, amend `docs/ULTIMA-DESIGN.md` §15 explicitly as part of that phase.

**Status: M9a, M9b done (2026-09-23).** M9a: pprof under `/debug/pprof/`
(`server.pprof_enabled`, metrics_allow + JWT guarded; `lib/httpapi/pprof.go`),
hardened harness `bin/bench-m9.sh`, baseline
`docs/benchmarks/M9-baseline-2026-09-23.md`. M9b (round 1): pipelined-burst
coalescing (`lib/commands/burst.go` — per-shard group fan-out via
`shard.DoTokAsync`, inline `do()` via `ConnState.inls`; disabled with
maxmemory set or in MULTI), pooled DoTok completion channels, AOF capture
check-only mode when appendonly is off, reader/execSegment scratch.
Campaign report `docs/benchmarks/M9-campaign-2026-09-23.md`: fixed-key GET
0.43×→0.88×, SET 0.44×→0.58×, random-key rows 1.05–1.37× vs Redis.

---

## Context

Current measured state (all on Apple M4 Max, 14 cores, 64 shards; reports in
`docs/benchmarks/`):

- GET/SET pipelined (-c50/-c200, -P16): **0.40–0.45× Redis**. Ratio *worsens*
  with more connections (0.45× at -c50 → 0.40× at -c200) → a
  contention/serialization point, not raw single-thread speed. PING is 0.80–
  0.94×, so the RESP front-end is near parity; the gap is dispatch/value-path
  (`note/M1-implemented.md`, never profiled).
- Pub/sub with 8 subscribers: 0.08–0.21×; the 4096-msg bounded push queue trips
  slow-consumer close where Redis's 32 MB buffer does not, and concurrent
  publishers contend on per-connection queue mutexes (`note/M3-implemented.md`).
- gRPC pipelined stream amortizes transport to ~2.8 µs/op vs 45 µs unary
  (`note/grpc-vs-text-benchmark/`); text-arg parsing is ~130–210 ns/op, 14
  allocs per SET.
- **No pprof anywhere** — the only mention is aspirational (§14.2). Profiling
  infrastructure is a hard prerequisite.
- Harness: `bin/bench*.sh` covers the M1 sweep, pub/sub, M5 eviction, M8
  scripting. Gaps: no memtier mixed-workload suite (§14.2 says
  "redis-benchmark + memtier"), single-run means (no repeats/variance), no
  payload-size or keyspace-size sweep outside M5.

§12 feature state (from code inventory):

- #2 per-shard metrics: `Engine.ShardStats()` (`lib/shard/shard.go`) + UI heatmap
  (`web/src/screens/Dashboard.tsx`) exist; Prometheus registry
  (`lib/httpapi/metrics.go`) is global-only; no per-shard command counters; no
  INFO shards section.
- #3 Batch API: `ExecBatch` exists on gRPC (`lib/grpcsrv/grpcsrv.go`,
  `clients/go/ultima/grpc.go`) — pipelined, non-atomic semantics. Nothing on WS,
  TS, or JS.
- #4 CDC: not started. Foundations: single emission point `notifyKeyspace`
  (`lib/commands/notify.go`, event name + key only), AOF capture point
  `capturePersist` (`lib/commands/aof.go`, sees every mutation), resumable
  sessions with bounded replay buffer (`lib/wssession/session.go`, in-memory,
  per-session).
- #5 multi-pattern SCAN/transforms: not started. SCAN family is single-`MATCH`
  glob (`lib/commands/keys.go`, `lib/commands/coll.go` `scanOpts`).

## M9a — Observability foundation — **done**

Everything else in the campaign depends on this. No behavior changes.

- pprof: mount `net/http/pprof` on the HTTP/WS mux under `/debug/pprof/`,
  gated by a new config flag `server.pprof_enabled` (default false) and the
  existing `metrics_allow` IP guard (`lib/httpapi/metrics.go` pattern). When
  auth is enabled, require a Bearer token like `/api/v1/*`.
- Harness hardening (`bin/bench.sh`, new `bin/bench-m9.sh`):
  - ≥3 repetitions per row, report median + spread (keep the existing
    hardware/version auto-record preamble).
  - Payload sweep `-d 3/64/1024` and the M1 rows (-c50/-c200, -P1/-P16) as the
    canonical target-workload matrix.
  - Add memtier_benchmark mixed-workload legs (e.g. 1:1 and 1:10 SET:GET) if
    memtier is available on the machine; skip gracefully otherwise (§14.2).
  - Profile capture: script wraps a run with
    `curl .../debug/pprof/profile?seconds=N` + heap/allocs/goroutine snapshots,
    saved next to the report.
- Baseline: run the hardened harness unmodified against current code; record as
  `docs/benchmarks/M9-baseline-<date>.md`. This is the denominator for all
  campaign deltas.

### Tests / gate

- New config field parses with defaults (`lib/config` tests); pprof endpoints
  404 when disabled, 200 guarded when enabled (extend `lib/httpapi` tests).
- Baseline report committed.

## M9b — Performance campaign, round 1: profile and fix the value path — **done**

Profile-driven; the hypotheses below are starting points, not commitments.
Each fix lands with a before/after harness run appended to the campaign log
(single running doc `docs/benchmarks/M9-campaign-<date>.md`).

- CPU + alloc profiles of the SET/GET -P16 workload. Documented suspects:
  1. Shard task handoff: closure allocation + channel round-trip per command
     (`Engine.Do`/`DoShard`) — candidates: sync.Pool for task/closure structs,
     batching multiple pipelined commands into one shard task (the connection
     already reads pipelined bursts; a `DoMulti`-style coalescing per shard
     amortizes the handoff).
  2. Per-connection read/parse loop in `lib/resp` — stay close to upstream (D2);
     prefer buffering/allocation fixes that upstream would take.
  3. Value path allocations (14 allocs/SET measured in
     `note/grpc-vs-text-benchmark`): argv `[][]byte` reuse, `resp.Value`
     pooling, avoiding string↔[]byte copies on the key path.
  4. crc64 key hashing per command — already required for routing; check it
     isn't recomputed between front-end, engine, and table.
- Checkpoint targets: parity (1×) then 2× on the target matrix. If 2× is not
  reachable via the above, stop and reassess the shard model question against
  §4.3 *before* continuing (see Risks).

### Tests / gate

- Full suite + differential green after each landed fix (no semantic change is
  acceptable — this phase is pure optimization).
- goleak clean; `-race` pass on `lib/shard` + `lib/commands`.

## M9c — Performance campaign, round 2: scaling and the gate

- Attack the -c50→-c200 regression (0.45×→0.40×): goroutine/scheduler profile,
  shard queue depth under load (extend `ShardStat` sampling if needed — feeds
  M9d), GOMAXPROCS/shard_count sweep on the 14-core machine.
- Pub/sub secondary target (not gating): replace the fixed 4096-cap push queue
  with a growable bounded queue (cap in bytes, Redis-like
  `client-output-buffer-limit pubsub` semantics, config
  `server.pubsub_client_buffer` with a Redis-compatible default) and reduce
  per-connection queue mutex contention on multi-publisher fan-out
  (`lib/pubsub`). Target: ≥1× Redis at 8 subscribers, zero dropped subscribers
  in `bin/bench-pubsub.sh`.
- Gate run: full target-workload matrix, median of ≥3 runs. **M9 gate met when
  every GET/SET row is ≥4× Redis.** Record pprof profiles alongside
  (`docs/benchmarks/M9-<date>.md`, drafted here, finalized in M9h).

### Tests / gate

- Pub/sub: extend `tests/` + `bin/bench-pubsub.sh` assertions — 8-subscriber
  leg must show 8/8 survivors and exact delivery counts.
- The 4× gate numbers are in the report.

## M9d — Per-shard metrics & heatmaps (§12.2, finish)

- Per-shard counters in the shard goroutine task loop (`lib/shard/shard.go`):
  commands executed per shard (cheap — one increment inside the owner
  goroutine, no atomics), exposed via `Engine.ShardStats()`.
- Prometheus (`lib/httpapi/metrics.go`): per-shard series
  `ultima_shard_keys{db,shard}`, `ultima_shard_mem_bytes{db,shard}`,
  `ultima_shard_queue_depth{db,shard}`, `ultima_shard_commands_total{db,shard}`.
  Behind `server.shard_metrics` (default true; the flag exists so high
  shard-count deployments can drop the cardinality).
- INFO: opt-in `INFO shards` section (precedent: `INFO script` — must **not**
  appear in plain INFO, or the differential gate breaks on the section set).
- Web UI (`web/src/screens/Dashboard.tsx`): heatmap already renders keys/memory;
  add commands/sec coloring mode using the new series (via the TS client; rerun
  `bun install` in `web/` if the client changes).

### Tests

- Unit: counters increment exactly once per command incl. MULTI/EXEC and EVAL
  fan-out paths; Prometheus output snapshot test; `INFO shards` present only on
  explicit request. Differential suite stays green (plain INFO unchanged).

## M9e — Batch API on WebSocket (§12.3)

- Proto (`proto/ultima/v1`): add a batch frame (repeated `Command` in one
  message, batch-correlated replies carrying per-item seq). `make gen_proto`;
  never hand-edit `gen/`.
- `lib/wssrv`: accept the batch frame, execute sequentially on the connection's
  `ConnState` via the engine (same pipelined, **non-atomic** semantics as gRPC
  `ExecBatch` — document that MULTI/EXEC remains the atomic path), one reply per
  item in order. Replies participate in the §9.4 replay buffer like any other.
- Clients: WS batch method in `clients/go/ultima` (mirroring
  `grpc.go:ExecBatch`), `clients/typescript` (then `bun install` in `web/` and
  `clients/javascript` dist rebuild), and `ultima-ws-cli` batch/file mode if
  trivial.
- REST parity check: if `lib/httpapi` gains nothing here, say so in the phase
  notes — do not grow the OpenAPI surface without need (D7/D11).

### Tests

- `tests/m9_ws_batch_test.go`: ordering, per-item errors don't abort the batch,
  seq correlation, replay-after-resume contains batch replies. TS round-trip
  (`tests/ts-roundtrip/`) covers the new frame. CLI matrix unaffected.

## M9f — Multi-pattern SCAN + server-side transforms (§12.5)

Base SCAN/HSCAN/SSCAN/ZSCAN replies stay byte-exact vs Redis 7.2.7 (differential
gate); extensions are additive syntax behind `server.scan_extensions` (default
false — §12: "each behind config flags").

- Multi-pattern: `SCAN cursor MATCHANY p1 p2 ... [MATCHALL p1 p2 ...]`
  (union / intersection of globs), and per-type scans `HSCAN ... MATCHANY ...`
  etc. via the shared `scanOpts` parser (`lib/commands/coll.go`). Rejected with
  a clear error when the flag is off.
- Transforms (Lua-free computed projections): `SCAN cursor TRANSFORM field ...`
  where field ∈ `TYPE`, `TTL`, `LEN` (STRLEN/HLEN/LLEN/SCARD/ZCARD/XLEN per
  value type), `ENCODING`-equivalent. Executed inside the owning shard
  goroutine, O(1) per key, no user code — deliberately not a Lua path.
- Cursor semantics unchanged (pluto cursor scan, `docs/pluto/11-hash-table-
  cursor-scan.md`); transforms add a payload column, they do not filter.
- Clients: Go typed helpers for the new options; document in the client READMEs.

### Tests

- New differential scripts (`tests/differential/scripts_m9.go`) proving the base
  forms are untouched when the flag is off (byte-exact incl. error strings);
  Ultima-only tests (`tests/m9_scan_test.go`) for MATCHANY/MATCHALL/TRANSFORM
  semantics, flag-off rejection, and interaction with COUNT/TYPE.

## M9g — Keyspace change feeds (CDC) (§12.4, rides on §9.4/D18)

"Durable-ish": a server-side, per-shard, seq-stamped in-memory change log with
offsets and global sequence merge (the `lib/persist/replay.go` merge pattern);
survives client disconnect via session resume, **not** server restart — state
this explicitly in docs and the API.

- Change log (`lib/cdc`, new package): per-shard ring buffer (count + age caps,
  config `cdc.buffer_max_msgs` / `cdc.buffer_max_ms`, mirroring the
  `auth.ws_replay_buffer_*` knobs), records `{seq, db, key, event, new_value?}`.
  `cdc.include_values` (default false) controls value capture — document the
  memory cost.
- Emission: single capture point next to `capturePersist`/`notifyKeyspace`
  (every mutation already funnels through the AOF capture path on the
  connection goroutine). Zero cost when `cdc.enabled` is false (one flag
  check); measure the enabled overhead with the M9a harness — budget: target
  workload stays ≥4× with CDC on.
- Subscribe APIs:
  - gRPC: new stream RPC `SubscribeChanges(start_seq)` in `proto/ultima/v1`
    with offset-ack frames; bounded queue + slow-consumer teardown like
    `Subscribe`.
  - WS: subscription frame type; delivery rides the §9.4 resumable session so a
    reconnecting client resumes from its last seq with no loss (within buffer
    retention; gap → explicit `SESSION_EXPIRED`-style error).
- Filtering: subscribe by db + key glob + event class (superset of
  `notify-keyspace-events` classes).
- Auth: when `auth.enabled`, subscription requires a Bearer token like other
  surfaces; add an allow-list hook if account-level filtering is cheap,
  otherwise document that CDC is all-or-nothing per account.

### Tests

- `tests/m9_cdc_test.go`: ordering under concurrent writers (per-shard order +
  global seq merge), resume-from-offset after WS disconnect (extend the §9.4
  recovery tests), buffer-overflow gap error, filter semantics, flag-off zero
  overhead (no log allocation). gRPC + WS round-trips; Go + TS client coverage.
- Benchmark: M9a harness re-run with `cdc.enabled=true` appended to the campaign
  doc.

## M9h — Final sweep, report, docs

- Re-run the full M9 gate matrix with all features landed (flags on and off) —
  the gate number must hold with the feature code present.
- Finalize `docs/benchmarks/M9-<date>.md`: target-workload table, pub/sub
  secondary numbers, harness description, hardware, pprof profiles, and the
  campaign narrative (what was found, what was fixed, what remains).
- Doc updates per house convention: M9 row + status in
  `docs/ULTIMA-DESIGN.md` §14.4 (and §12 items annotated delivered/deferred),
  narrative paragraph + `## M9 architecture notes` in
  `docs/implementation-history.md`, AGENTS.md (new packages `lib/cdc`, config
  flags, bench-m9.sh, any new commands), any new `docs/pluto/` spec if pluto
  changed.

## Validation (M9 done gate)

1. `make build` / `make build-cli`; `CGO_ENABLED=0 go build ./...`.
2. `go test ./...` green — incl. differential byte-exact suite and
   `tests/m9_*_test.go`; goleak clean.
3. `make lint` clean; `make test-web` if the dashboard changed; TS/JS clients
   build (`make test-clients`).
4. `make test-cli-matrix` green in both security modes.
5. `docs/benchmarks/M9-<date>.md` shows **≥4× Redis** on every GET/SET row of
   the target matrix (median of ≥3 runs, 8+ cores), pub/sub secondary target
   met, pprof profiles attached.
6. Docs updated per M9h.

## Out of scope for M9

- §12.6 pluggable persistence backends / `b_tree_disk_ts` warm tier (deferred;
  the pluto structure exists and `lib/persist` has no storage-backend interface
  yet — both noted for the follow-up milestone).
- EVAL throughput parity (structural: wazero R3 interpreted on darwin/arm64;
  revisit if the wasm runtime gains a compiler backend).
- Pub/sub ≥4× (secondary target is ≥1× with zero slow-consumer drops).
- Redis-compatible RDB export (§13.1 stretch), ACLs, clustering/replication.

## Risks / open points

- **The 4× gate is ~9× away** (0.40–0.45× today). Parity-class fixes (M9b)
  plausibly reach 1–2×; beyond that may require structural work — per-shard
  command coalescing, multiple connections per shard pipeline, or revisiting
  the one-goroutine-per-shard drain model. If a fix would alter §4.1/§4.3 or
  any D-decision, it must be proposed as an explicit §15 amendment, not landed
  silently. Falling short is reported honestly in the final report with
  profiles showing where the remaining time goes; the workload definition
  (§1.2) is not narrowed to make the number.
- **CDC hot-path cost**: emission sits on every mutation. Mitigated by the
  flag-off zero-cost requirement and the M9g benchmark budget; if the enabled
  cost breaks the 4× gate, `include_values` and filter pushdown are the first
  knobs, per-shard log batching the second.
- **Shard-metric cardinality**: 64 shards × series is fine; very high
  `shard_count` deployments are why `server.shard_metrics` exists.
- **Pub/sub growable queue** reintroduces unbounded-memory risk that the
  4096 cap was avoiding — the byte cap + Redis-like disconnect-on-limit
  semantics must be enforced and tested, not just defaulted.
- **Harness comparability**: all campaign numbers must come from the same
  machine class as the baseline (M4 Max today); a hardware change mid-campaign
  invalidates comparisons — re-baseline instead.
