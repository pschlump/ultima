# ULTIMA — Design Document

**Ultima** is a superset clone of Redis written in Go: a drop-in replacement for the
Redis wire protocol and command set, plus additional access protocols (gRPC and
WebSocket/protobuf), plus a web-based management UI. The driving goal is
**substantially higher throughput than Redis** by replacing Redis's single-threaded
command execution with a parallel, goroutine-based architecture.

- Status: Draft v0.1
- Reference sources: Redis `unstable` branch (version **8.9.241**) checked out at
  `./note/redis`; data-structure library `github.com/pschlump/pluto` at `../pluto`;
  layout/tooling model project `github.com/Agentic-Quartz/exsms`.

---

## 1. Goals and Non-Goals

### 1.1 Goals

1. **Drop-in replacement**: any standard Redis client (redis-cli, go-redis, redis-py,
   node_redis, jedis, …) can connect to Ultima's RESP port and work unmodified.
   RESP2 first, RESP3 negotiated via `HELLO 3`.
2. **High throughput**: parallel command execution across goroutines, targeting a
   multiple of single-instance Redis throughput on multi-core hardware for the
   common read/write workloads (target: ≥ 4× Redis on 8+ cores for GET/SET-heavy
   mixes, measured with `redis-benchmark`).
3. **Binary access protocols**: gRPC and WebSocket endpoints carrying a protobuf
   request/response protocol, so high-performance clients skip RESP text parsing
   entirely.
4. **Superset features**: beyond Redis parity (management HTTP API, richer metrics,
   per-shard introspection) — see §10.
5. **Management web UI**: React + bun + vite app in `./web`, served from the
   HTTP/WebSocket port.

### 1.2 Non-Goals (initially)

- Redis Cluster mode (gossip bus, slot migration). Ultima starts as a single-node
  server; horizontal scale-out is a later phase (§12).
- Sentinel.
- Exact memory-footprint parity with Redis. Redis's listpack/intset/quicklist
  encodings are memory optimizations tuned for C; Go versions would be a major
  project of their own. Ultima optimizes for **throughput**, accepting a higher
  RAM per key (documented in §5.4).
- Byte-for-byte identical RDB/AOF file formats (we provide compatible
  save/restore semantics; format compatibility is a stretch goal).
- Lua scripting is a parity goal but scheduled late (§12); Redis Functions
  (FCALL) are out of scope for v1.

---

## 2. What Redis Actually Does (and What We Replace)

From the checked-out Redis 8.9.241 source (`note/redis/src/`):

- **Event loop**: `ae.c` wraps epoll/kqueue; `aeMain()` loops over
  `aeProcessEvents`. `beforeSleep` (`server.c:1956`) batches client reply flushes,
  handles blocked clients, active-expire accounting. Command execution is
  single-threaded on the main loop; `iothread.c` optionally offloads socket
  read/write; `bio.c` runs fsync/close/lazy-free in background threads.
- **Dispatch**: `processInputBuffer()` (`networking.c:3778`) parses inline and
  multibulk RESP, then `processCommand` → `lookupCommand` against a table built
  from `commands.def` (458 `MAKE_CMD` entries, ~250+ top-level commands) →
  `c->cmd->proc(c)`.
- **Data types**: string (`t_string.c`), hash (`t_hash.c`, listpack/HT, plus new
  template-hash encodings), list (`t_list.c`, quicklist), set (`t_set.c`,
  intset/listpack/HT), zset (`t_zset.c`, listpack or skiplist+dict pair), stream
  (`t_stream.c`, rax + listpacks), and the new Array type (`t_array.c`).
- **Expiry**: passive (`expireIfNeeded`, `db.c:48`) on every access + active
  time-boxed sampling (`activeExpireCycle`, `expire.c:295`) driven by
  `serverCron`, with `ebuckets.c` expiry buckets.
- **Eviction**: `evict.c` — `volatile-lru/lfu/ttl/random`, `allkeys-lru/lfu/random`,
  `noeviction`; approximate LRU via sampling, LFU via Morris counters.
- **Persistence**: `rdb.c` (fork-based BGSAVE, LZF, CRC64), `aof.c` (MP-AOF,
  everysec fsync via bio threads, fork-based rewrite).

**Ultima replaces**: the epoll loop with goroutine-per-connection (Go's net
poller makes this free); the single command thread with a **sharded keyspace,
one owner goroutine per shard** (§4); `beforeSleep` batching with buffered
per-connection writers; bio threads with ordinary goroutines. The unit of work
that parallelizes is exactly `processCommand`.

---

## 3. Server Topology — Ports and Listeners

One process, three network surfaces, each with its own port (all configurable):

| Surface | Default port | Purpose |
|---|---|---|
| **RESP port** | 6379 | Redis-compatible wire protocol (RESP2/RESP3, TLS optional, Unix socket optional). Drop-in replacement surface. |
| **gRPC port** | 6380 | Protobuf service API — typed, binary, no parsing. Streaming support for pub/sub and monitor-like feeds. |
| **HTTP/WS port** | 6381 | chi-based HTTP mux: management/monitoring REST API (OpenAPI 3.0), `/metrics` (Prometheus), health checks, the React web UI, **and** the WebSocket endpoint for binary protobuf command access. |

Rationale for combining HTTP + WebSocket on one port: they share the chi mux
(WS upgrade handled by a chi route), one TLS stack, one middleware chain, one
set of ops endpoints. The RESP and gRPC ports stay dedicated for max throughput.

---

## 4. Concurrency Architecture (the core design)

### 4.1 Sharded keyspace with owner goroutines

The keyspace is split into **N shards** (default: `4 × GOMAXPROCS`, rounded up to
a power of two; configurable). Each shard owns:

- its own hash table (`pluto/cuckoo_ts` or `pluto/hash_grow_ts` — see §5),
- its own expiry min-heap (`pluto/heap_ts`),
- its own stats counters.

Routing: `shard = crc64(key) & (N-1)` (pluto `crc` package). All commands that
touch a single key execute **inside the owning shard's goroutine**, which
serializes per-key access with **zero locks on the hot path** — the same safety
Redis gets from its single thread, but N-way parallel.

```
conn goroutine ──parse──► route by key ──► shard queue ──► shard goroutine ──► reply
```

### 4.2 Execution model per command class

| Command class | Execution |
|---|---|
| Single-key (GET/SET/HSET/LPUSH/…) | Dispatched to the key's shard goroutine; fully parallel across shards. |
| Multi-key same-shard (MSET/MGET/DEL when all keys hash together) | Single shard task, atomic within shard. |
| Multi-key cross-shard (MGET, DEL, EXISTS, cross-slot rename) | Fanned out to shards, results joined by the connection goroutine. **Documented semantic**: cross-shard commands are *not* transactional across shards in v1 fast path (per-shard atomicity only), matching what clients tolerate from Cluster mode. A strict mode (two-phase shard locking, Redis-exact semantics) is available via config for DROP-IN strictness. |
| Keyspace-wide (KEYS, SCAN, DBSIZE, FLUSHALL, RANDOMKEY) | SCAN iterates shard-by-shard with cursors `shard:inner-cursor`; KEYS/FLUSHALL fan out. |
| Pub/Sub | Dedicated broker goroutine (per-channel sharded if needed); SUBSCRIBE moves the connection into push mode. |
| Transactions (MULTI/EXEC) | All commands of a transaction are coalesced and executed with the strict cross-shard path (shard locks taken in shard-id order to avoid deadlock). WATCH uses per-shard version counters. |
| Blocking (BLPOP, XREAD BLOCK, …) | The *wait* never occupies a shard goroutine: the command registers interest and parks the connection goroutine; shard events wake waiters via channels. |

### 4.3 Why owner-goroutines instead of striped locks

Lock-striping also works, but message-passing to an owner goroutine gives us:
cache-friendly single-writer access to each shard's structures, natural batching
(drain the shard queue in a burst), trivially correct compound operations via
pluto's `Lock()`/`Nl*` no-lock-method pattern if we ever switch a shard to a
directly-shared `_ts` structure, and clean per-shard metrics. Benchmarks in
milestone M1 will validate this against a striped-lock prototype; the command
dispatch layer abstracts the choice.

### 4.4 Connection handling

- Goroutine per connection on the RESP port (via redcon, §6.1) and per stream on
  gRPC; per WS connection on the HTTP/WS port.
- Replies are written through a per-connection buffered writer with an explicit
  flush policy (flush on empty shard-queue wait, or at a size threshold) to
  recover the syscall batching Redis gets from `beforeSleep`.
- Pipelining is fully supported: parse-ahead, dispatch-ahead, in-order replies
  per connection (reply sequencing tokens).

---

## 5. Data Structures — pluto Mapping, Additions, and Gaps

pluto (`github.com/pschlump/pluto`, Go 1.27, zero deps, generics, range-over-func
iterators) rule: packages are goroutine-safe **only** if suffixed `_ts`
(internal `sync.RWMutex`; ten also expose `Lock()`/`Unlock()` + `Nl*` no-lock
methods for atomic compound ops).

### 5.1 Redis type → pluto structure

| Redis type | Ultima encoding | pluto package(s) |
|---|---|---|
| Keyspace (per shard) | Hash: key → `*Entry` (type tag, value ptr, expire-at, version) | `cuckoo_ts` (O(1) guaranteed search/delete, background resize) — benchmark against `hash_grow_ts` |
| String | `[]byte` + int-detection for INCR fast path | stdlib |
| Hash | small: sorted slice or `dll` of field/value pairs; large: hash of fields | `hash_grow_ts` / `cuckoo_ts` per-hash, or Go map under shard lock |
| List | deque | `dqueue_ts` (Push/Pop both ends); large lists may need segmented deque (gap, §5.3) |
| Set | small-int: sorted int slice (intset equivalent); large: hash with unit values | `hash_grow_ts`/`cuckoo_ts` |
| Sorted set | skiplist + hash pair (like Redis large zset) | `skip_list_ts` + `hash_grow_ts` — **needs rank/range API added (§5.3)** |
| Stream | append-only segmented log keyed by ID | **gap — new structure needed** |
| Expiry (per shard) | min-heap on expire-at ms | `heap_ts` |
| Eviction (LRU) | capacity-bounded LRU | `lru` (wrap in shard lock; not `_ts`) |
| RDB checksums | CRC-64 | `crc` |
| Glob matching for KEYS | wildcard matching | `trie.KeysThatMatch` or simple glob (Redis uses `stringmatchlen`) |

### 5.2 Expiry design (replacing expire.c/ebuckets.c)

- **Passive**: every shard access checks `expireAt` before use (same as
  `expireIfNeeded`).
- **Active**: each shard goroutine runs a periodic time-boxed sweep popping its
  `heap_ts` expiry heap (replaces Redis's sampling cycle; a heap gives exact
  earliest-expiry ordering at O(log n) per expire instead of sampled buckets).
- Hash-field TTLs (Redis 7.4+/8 feature, `HEXPIRE` etc.) stored in a per-hash
  mini-heap — phase 2.

### 5.3 Gaps — structures to request from the pluto side

The following do not exist in pluto today and are needed for full parity
(user has offered to build these; listed in priority order). Each has a
detailed requirements/prompt document in `docs/pluto/`:

1. **Sorted-set range/rank ops on skip_list** (`Range(lo,hi)`, `Rank`, `Ceil`,
   `Floor`, by-index access). Blocks: ZRANGEBYSCORE, ZRANK, ZREMRANGEBYRANK, …
   Currently only full-scan iterators exist.
   → `docs/pluto/01-skip-list-range-rank.md`
2. **Stream structure**: radix-ordered map of stream IDs → packed entry blocks,
   with consumer-group metadata. Blocks: XADD/XRANGE/XREAD/XGROUP…
   → `docs/pluto/02-stream.md`
3. **Sharded concurrent hash table** (single logical table, internal striping) —
   optional; Ultima can shard at the keyspace layer with N `cuckoo_ts` tables,
   but a native striped table with a unified SCAN cursor would simplify
   keyspace-wide iteration.
   → `docs/pluto/03-sharded-hash-table.md`
4. **HyperLogLog** (or a good cardinality estimator). Blocks: PFADD/PFCOUNT/PFMERGE.
   → `docs/pluto/04-hyperloglog.md`
5. **LFU counter structure** (Morris-counter approx frequency) for
   `allkeys-lfu`/`volatile-lfu` eviction. LRU is covered by `lru`; LFU is not.
   → `docs/pluto/05-lfu-counter.md`
6. **Thread-safe LRU** (`lru_ts`) — minor; currently callers must lock.
   → `docs/pluto/06-lru-ts.md`
7. **Thread-safe patricia/radix trie** (`patricia_trie_ts`) — for keyspace prefix
   ops and possibly the stream ID index.
   → `docs/pluto/07-patricia-trie-ts.md`
8. **Geo helpers** (geohash encode/decode + neighbor search on a sorted set).
   Blocks: GEOADD/GEOSEARCH… (Redis implements GEO on zset; we can too once #1 lands).
   → `docs/pluto/08-geo.md`
9. **Bitmap/bitfield helpers** — thin; can be plain `[]byte` ops in Ultima, but a
   shared package avoids duplication.
   → `docs/pluto/09-bitmap-bitfield.md`
10. **Bounded segmented deque** for very large lists (quicklist equivalent) —
    memory-efficiency phase, not v1.
    → `docs/pluto/10-segmented-deque.md`
11. **Cursor-based incremental Scan on hash tables** (`hash_grow`, `cuckoo` +
    `_ts` twins) — Redis-`dictScan`-style cursors that survive resize; needed
    for SCAN/HSCAN/SSCAN/ZSCAN without whole-shard snapshots.
    → `docs/pluto/11-hash-table-cursor-scan.md`

### 5.4 Memory expectations

Go maps/headers cost more per key than Redis's listpack/intset encodings.
Expect ~1.5–2.5× RAM per key versus Redis for small values. Ultima documents
`MEMORY USAGE` equivalents and offers `OBJECT ENCODING`-style introspection over
its own encodings. If footprint becomes a priority, a listpack-like packed
encoding for small hashes/sets/lists is a later optimization (§12).

---

## 6. Protocol Layers

### 6.1 RESP (Redis-compatible port)

**Decision: build on `github.com/tidwall/redcon` (MIT), vendored/forked as
`lib/resp`.**

Why redcon:
- Purpose-built, battle-tested RESP server framework: pipelining, inline/telnet
  commands, TLS, pub/sub helpers, `redcon.PubSub`.
- Demonstrated faster than Redis itself on GET/SET benchmarks
  (redcon README: ~2.0M SET/s / 4.0M GET/s multi-threaded vs Redis ~0.94M/1.19M
  on the same hardware — old numbers, but the architecture is proven).
- Tiny, readable codebase (`redcon.go`, `resp.go`) — easy to modify.

Required modifications:
1. **RESP3 support**: redcon is RESP2-only. We add RESP3 reply types (map `%`,
   set `~`, double `,`, big number `(`, boolean `#`, null `_`, verbatim `=`,
   push `>`) and `HELLO` negotiation. Options: (a) extend the forked redcon
   writer with RESP3 emitters and per-connection protocol version, replies
   downgraded for RESP2 clients exactly as Redis does in
   `networking.c:1308`; or (b) layer `github.com/tidwall/resp` (lower-level
   reader/writer) under our own server loop if the fork proves limiting.
   Plan: (a), with (b) as fallback.
2. Hook points for our shard dispatch (redcon calls one handler func per
   command — perfect fit).
3. Connection context for MULTI state, WATCH versions, pub/sub mode, RESP
   version, auth state, client name (`CLIENT SETNAME`), selected DB number.

### 6.2 gRPC + protobuf protocol

- IDL in `proto/ultima/v1/*.proto`; generated to `gen/go` (and `gen/ts` for the
  web UI) via `bin/gen.sh` (same pattern as exsms).
- Core service sketch:

```proto
service Ultima {
  rpc Exec(CommandRequest) returns (CommandResponse);          // unary single command
  rpc ExecBatch(BatchRequest) returns (BatchResponse);          // pipelined batch, ordered
  rpc Subscribe(SubscribeRequest) returns (stream PushEvent);   // pub/sub + keyspace notifications
  rpc Monitor(MonitorRequest) returns (stream CommandEvent);    // MONITOR equivalent
}
message CommandRequest { string command = 1; repeated bytes args = 2; uint64 db = 3; }
```

- Every gRPC command maps 1:1 onto the same internal command executor as RESP —
  the RESP layer is only a *parse/serialize* shim over the executor. This is the
  key architectural invariant: **one command engine, three front-ends**.
- Value representation in protobuf mirrors RESP3 types (null, int, double,
  blob-string, array, map, error) so gRPC clients get RESP3-grade fidelity.

### 6.3 WebSocket (binary protobuf) on the HTTP/WS port

- `gorilla/websocket` (as in exsms) at `/ws/v1` (chi route).
- Frames carry the same protobuf `CommandRequest`/`CommandResponse` messages as
  gRPC — a browser-friendly binary channel with no RESP parsing. Text JSON frames
  optionally supported for debugging.
- The web UI itself uses this endpoint for live dashboards (plus REST for CRUD).

---

## 7. Command Coverage Plan

Redis 8.9.241 ships ~458 command-table entries (~250+ top-level commands).
Coverage is tracked in a machine-readable manifest
(`lib/commands/manifest.json` — name, group, status, since-version, notes),
generated docs page, and enforced by tests. Phased:

| Phase | Groups | Representative commands |
|---|---|---|
| **P0 — core KV** | connection, server (subset), string, keyspace, generic | PING, HELLO, AUTH, SELECT, SET/GET/DEL/EXISTS/EXPIRE/TTL/TYPE/SCAN/INCR/APPEND/GETSET/MGET/MSET, INFO, DBSIZE, FLUSHDB, CONFIG GET/SET (subset), CLIENT (subset) |
| **P1 — collections** | hash, list, set, sorted-set | H*, L*, S*, Z* (needs pluto gap #1) |
| **P2 — transactions & pub/sub** | MULTI/EXEC/WATCH, SUBSCRIBE/PUBLISH/PSUBSCRIBE/SSUBSCRIBE, keyspace notifications | |
| **P3 — streams, scripting-lite, persistence cmds** | X*, SAVE/BGSAVE/BGREWRITEAOF/LASTSAVE, EVAL via a Go-embedded Lua (gopher-lua) or defer | |
| **P4 — parity tail** | BITOP/BITFIELD, GEO*, PF*, OBJECT, MEMORY, DEBUG (subset), hash-field TTLs (HEXPIRE…), ACL (subset), SORT | |
| **P5 — superset** | see §10 | |

Full per-command semantics (error strings, arity, edge cases like
`INCR` overflow, `SET` option combinations) are validated against real Redis via
a differential test harness (§11).

---

## 8. Configuration

Modeled directly on exsms `lib/config/config.go`:

- JSON config file, `--cfg` flag; `default:"..."` struct tags applied via
  reflection *before* unmarshal (file overrides defaults); `$ENV$NAME`
  substitution (and `$ETCD$/key` if we want it later) via `substituteEnvRefs`.
- `Config` struct groups: `server` (ports, TLS, shard count, limits, eviction
  policy, persistence paths), `debug` (`enabled map[string]bool` feature flags).
- Build stamping via `bin/gen-build-stamp.sh` → `-ldflags -X
  main.GitCommit/Version/BuildDate/GitBranchName/BuildTarget`, placeholder vars
  in `cmd/ultima-server/version.go`.
- Startup-posture check (refuse unsafe configs: RESP on 0.0.0.0 with no auth +
  no TLS, etc.), same philosophy as exsms `checkStartupPosture`.
- Also accept a subset of classic `redis.conf` keys via a translation flag
  (`--redis-conf path`) to ease drop-in migration — translation layer maps to
  the JSON model.

Example sketch:

```json
{
  "server": {
    "resp_addr": ":6379",
    "grpc_addr": ":6380",
    "http_addr": ":6381",
    "shard_count": 0,
    "max_memory_mb": 0,
    "eviction_policy": "noeviction",
    "requirepass": "$ENV$ultima_password",
    "aof_enabled": true,
    "aof_fsync": "everysec"
  },
  "debug": { "enabled": { "dump.commands": false } }
}
```

---

## 9. HTTP Management API + Web UI

### 9.1 HTTP API (chi + OpenAPI 3.0 + validator)

Following exsms conventions:

- `api/openapi.yaml` is the contract source of truth; embedded and served at
  `/api/openapi.yaml`; Swagger UI served from embedded assets
  (`lib/httpapi`). Routes hand-registered on chi in
  `lib/handler.RegisterRoutes` (exsms generates `chi-server` bindings but
  doesn't wire them; we either wire oapi-codegen properly or hand-register from
  day one — decide at M0; hand-registration is the lower-friction default).
- Request DTOs validated with `go-playground/validator/v10`, decoded through a
  `JsonBody` helper that reuses the config `SetDefaults` `default:`-tag
  mechanism (exsms `lib/utils/jsonbody.go` pattern).
- Middleware: request-id → request logging (`slog` JSON) → real-IP (trusted
  proxies only) → Recoverer → Timeout → Prometheus (`chiprom`-style).
- Endpoint groups (all under `/api/v1`):
  - `GET /health`, `GET /ready`
  - `GET /metrics` (Prometheus, IP-allowlist guarded)
  - `GET /api/v1/info` — INFO as structured JSON
  - `GET /api/v1/shards` — per-shard key counts, ops/sec, queue depth, expiry
    heap size, memory
  - `GET /api/v1/clients`, `POST /api/v1/clients/{id}/kill`
  - `GET /api/v1/slowlog`, `GET /api/v1/latency`
  - `GET|PUT /api/v1/config` — runtime config (CONFIG GET/SET equivalents)
  - `POST /api/v1/flushdb`, `POST /api/v1/save`, `POST /api/v1/bgsave`
  - `GET /api/v1/keys/scan?cursor=…&match=…&count=…`, `GET /api/v1/key/{key}`
    (typed value preview), `DELETE /api/v1/key/{key}`
- Auth: bearer token / mTLS for the management surface, independent of RESP
  AUTH.

### 9.2 Web UI (`./web`, React + bun + vite)

- Vite + React + TypeScript; bun for package management and scripts
  (`bun install`, `bun run dev`, `bun run build` → `web/dist`, embedded into the
  Go binary via `//go:embed` for production serving).
- Screens: dashboard (ops/sec, memory, hit ratio, per-shard heatmap), key
  browser (scan + typed value viewer/editor), live monitor (over WS), slowlog,
  config editor, pub/sub inspector.
- Communicates via the REST API + `/ws/v1` binary protobuf (ts-proto generated
  clients in `web/src/proto`, same gen pipeline as exsms).

---

## 10. Superset Features (beyond Redis parity)

Planned after P4, each behind config flags:

1. **gRPC/WS binary protocol** (already core — §6.2/§6.3).
2. **Per-shard metrics & heatmaps** — impossible in stock Redis.
3. **Batch API** (`ExecBatch`) with server-side pipelining semantics.
4. **Keyspace change feeds** (durable-ish CDC stream over WS/gRPC — richer than
   keyspace notifications).
5. **Multi-pattern SCAN** and server-side Lua-free computed transforms.
6. **Pluggable persistence backends** (§11.3) — including pluto
   `b_tree_disk_ts`-backed warm tier for overflow (values are `uint64` there, so
   it stores an indirection to blob files; experimental).

---

## 11. Persistence, Eviction, and Ops Semantics

### 11.1 Persistence

- **Snapshot (RDB-equivalent)**: per-shard consistent snapshot via shard-goroutine
  pause-and-drain (milliseconds per shard, staggered — no fork needed in Go),
  serialized in our own compact format with `crc` CRC-64 checksums; LZF or
  zstd compression. Optionally emit Redis-compatible RDB later (stretch).
- **AOF-equivalent**: per-shard append logs + manifest (MP-AOF-like), fsync
  policies `always/everysec/no` on a background goroutine (bio replacement);
  rewrite = compact replay into new logs.
- Restore on startup, `SAVE`/`BGSAVE`/`BGREWRITEAOF`/`LASTSAVE` commands, plus
  HTTP triggers.

### 11.2 Eviction

- `maxmemory` with policies `noeviction`, `allkeys-lru` (pluto `lru` per shard),
  `volatile-lru`, `volatile-ttl` (expiry heap order), `allkeys-random`,
  `volatile-random`; LFU policies deferred until pluto gap #5 lands.
- Approximate LRU sampling (Redis-style) vs exact LRU (pluto `lru`) — benchmark
  both; exact per-shard LRU is cheap enough under owner-goroutine model.

### 11.3 Databases

Multiple logical DBs (SELECT 0..15) supported via per-DB shard sets; FLUSHDB is
per-DB. (Configurable max DBs; default 16 like Redis.)

---

## 12. Project Layout, Tooling, Milestones

### 12.1 Layout (modeled on exsms)

```
ultima/
├── cmd/
│   ├── ultima-server/        # main binary (main.go, version.go, startup.go, router.go)
│   ├── ultima-bench/         # benchmark driver
│   └── ultima-cli/           # operator CLI (later)
├── lib/
│   ├── config/               # FromFile/SetDefaults/$ENV$ (exsms pattern)
│   ├── resp/                 # vendored+modified redcon (RESP2→RESP3)
│   ├── shard/                # keyspace shards, owner goroutines, routing
│   ├── commands/             # command table, executors, manifest.json
│   ├── types/                # string/hash/list/set/zset/stream encodings (pluto-backed)
│   ├── expire/               # passive+active expiry
│   ├── evict/                # maxmemory policies
│   ├── persist/              # snapshot + AOF
│   ├── pubsub/               # broker
│   ├── grpcsrv/              # gRPC front-end
│   ├── wssrv/                # WebSocket front-end
│   ├── handler/              # HTTP API (chi), embedded openapi.yaml
│   ├── metrics/              # Prometheus
│   └── reqlog/ utils/        # middleware, JsonBody helpers
├── api/openapi.yaml          # HTTP contract (+ oapi-codegen.yaml if used)
├── proto/ultima/v1/          # protobuf IDL
├── gen/go, gen/ts            # generated protobuf code
├── web/                      # React + bun + vite management UI
├── tests/                    # integration + differential tests vs real Redis
├── bin/                      # gen.sh, gen-build-stamp.sh
├── docs/                     # this file, protocol notes, benchmark reports
├── note/redis/               # Redis reference source (gitignored)
├── Makefile                  # default goal: build
└── .golangci.yml             # v2 lint config (exsms-tuned)
```

Key dependencies: `go-chi/chi/v5`, `go-playground/validator/v10`,
`gorilla/websocket`, `google.golang.org/grpc` + `protobuf`,
`prometheus/client_golang`, `tidwall/redcon` (vendored into `lib/resp`),
`pschlump/pluto`, `go.uber.org/goleak` (tests). Go 1.27.

### 12.2 Build tooling (Makefile, mirroring exsms)

- `make gen_proto` (bin/gen.sh → protoc Go+TS, copy into web/src)
- `make generate` (oapi-codegen, if adopted) / `make sync-openapi` (embed copy +
  drift check, prerequisite of server builds)
- `make build` (build stamp + all binaries), `make run` (dev config in tests/),
  `make test` (`go test ./...`, integration in `tests/`), `make lint`
  (golangci-lint v2), `make tidy`, `make clean`
- `make bench` — redis-benchmark + memtier suite, results committed to
  `docs/benchmarks/`

### 12.3 Testing strategy

1. **Unit tests** colocated (`lib/.../*_test.go`), goleak for goroutine leaks.
2. **Integration tests** in `tests/`: real client (go-redis) against a spawned
   server.
3. **Differential testing**: harness runs the same command sequences against
   Ultima and real Redis 8.x (from `note/redis`, built locally) and diffs
   replies, including error strings — the primary parity gate. Redis's own TCL
   suite (`note/redis/tests/`) is reused where feasible by pointing it at the
   Ultima port.
4. **Concurrency stress**: randomized multi-client workloads under `-race`,
   invariant checkers (linearizability spot checks per key).
5. **Benchmarks**: `redis-benchmark -P/-c` sweeps vs local Redis, per milestone;
   memtier for mixed workloads; pprof profiles attached to reports.

### 12.4 Milestones

| MS | Deliverable | Exit criteria |
|---|---|---|
| **M0** | Skeleton: config, logging, three listeners (stub), Makefile, lint, CI-local | `make build test lint` green; PING on all three surfaces |
| **M1** | Shard engine + RESP front-end + P0 commands; redcon fork w/ RESP3 | redis-cli fully works for P0; differential harness green on P0; first benchmark report vs Redis |
| **M2** | P1 collections (needs pluto gap #1 for full zset) | differential green on H/L/S/Z |
| **M3** | P2 transactions + pub/sub + blocking ops | MULTI/EXEC + WATCH stress green; redis-benchmark pub/sub |
| **M4** | gRPC + WS front-ends; proto IDL stable | go + ts clients round-trip; parity with RESP replies |
| **M5** | Expiry hardening, eviction, persistence (snapshot + AOF) | crash-recovery tests; maxmemory soak |
| **M6** | HTTP API + web UI v1 | dashboard live; key browser works end-to-end |
| **M7** | P3/P4 parity tail (streams, Lua-lite, bitfield, geo, PF*) | differential green on covered tail |
| **M8** | Superset features (§10) + performance campaign | ≥4× Redis on target workload; final report |

---

## 13. Key Decisions (summary)

| # | Decision | Rationale |
|---|---|---|
| D1 | Goroutine-per-connection + sharded keyspace with owner goroutines (no hot-path locks) | Directly replaces ae.c/single-thread with N-way parallelism; per-key serialization preserved |
| D2 | Vendor & extend `tidwall/redcon` as `lib/resp`, add RESP3 | Proven RESP2 server faster than Redis in benchmarks; small codebase; MIT |
| D3 | One command engine, three front-ends (RESP/gRPC/WS) | No logic duplication; parity bugs fixed once |
| D4 | pluto `_ts` structures inside owner-goroutine shards (locks mostly uncontended) | Reuse tested generics; `Lock`/`Nl*` available for compound ops |
| D5 | Heap-based exact expiry per shard instead of sampled active expiry | Simpler + exact; `heap_ts` exists today |
| D6 | Config = JSON + `default:` tags + `$ENV$` (exsms pattern) | Consistency with existing tooling; reflection machinery proven |
| D7 | HTTP API contract-first (`api/openapi.yaml`, chi, validator) | exsms pattern; doc-drift tests |
| D8 | No Redis Cluster/Sentinel in v1; single-node, many cores | Focus throughput goal; scale-out later |
| D9 | Own snapshot/AOF format first, RDB-compat later | Ship persistence early without format reverse-engineering |
| D10 | Memory footprint parity explicitly traded for throughput | Go encodings cost more RAM; documented |

## 14. Open Questions

1. oapi-codegen: wire generated chi-server bindings properly, or hand-register
   routes as exsms does? (decide at M0)
2. Strict vs fast cross-shard multi-key semantics as default? (current plan:
   fast default, strict config flag)
3. Lua engine for EVAL: `gopher-lua` (pure Go, slower) vs defer scripting?
4. Exact-LRU vs sampled-LRU eviction — benchmark in M5.
5. Native sharded table from pluto (gap #3) vs N independent `cuckoo_ts` — M1
   benchmark decides.
