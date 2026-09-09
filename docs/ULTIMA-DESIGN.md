# ULTIMA — Design Document

**Ultima** is a superset clone of Redis written in Go: a drop-in replacement for the
Redis wire protocol and command set, plus additional access protocols (gRPC and
WebSocket/protobuf), plus a web-based management UI. The driving goal is
**substantially higher throughput than Redis** by replacing Redis's single-threaded
command execution with a parallel, goroutine-based architecture.

- Status: Draft v0.4 (all pluto data-structure gaps now implemented, §5.3;
  authentication/login system with TOTP 2FA and multiple admin accounts added, §9;
  WebSocket connection recovery added, §9.4; TypeScript/JavaScript client libraries
  and example applications added, §11; interactive web console added, §10.2;
  command-renaming excluded as security by obscurity, §1.2)
- Reference sources: Redis `unstable` branch (version **8.9.241**) checked out at
  `./note/redis`; data-structure library `github.com/pschlump/pluto` at `../pluto`;
  TOTP/2FA library `github.com/pschlump/htotp` at `../htotp`;
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
   per-shard introspection) — see §12.
5. **Management web UI**: React + bun + vite app in `./web`, served from the
   HTTP/WebSocket port, including an interactive command console (§10.2).
6. **Real authentication**: a login system layered on the existing user/ACL base —
   multiple administrative accounts, optional TOTP two-factor authentication, and
   JWT access/refresh tokens covering the HTTP, WebSocket, and gRPC surfaces (§9).
   WebSocket sessions are resumable: a client that loses its connection recovers
   without losing pushed messages (§9.4).
7. **Client libraries**: official Go, TypeScript, and JavaScript libraries for
   the access protocols, shipped with complete example applications (§11). The
   Go library also serves as the foundation of the operator CLIs (§6.4).

### 1.2 Non-Goals (initially)

- Redis Cluster mode (gossip bus, slot migration). Ultima starts as a single-node
  server; horizontal scale-out is a later phase (§14).
- Sentinel.
- Exact memory-footprint parity with Redis. Redis's listpack/intset/quicklist
  encodings are memory optimizations tuned for C; Go versions would be a major
  project of their own. Ultima optimizes for **throughput**, accepting a higher
  RAM per key (documented in §5.4) — mitigated for large lists by pluto's
  `quicklist_ts` (§5.3 #10).
- Byte-for-byte identical RDB/AOF file formats (we provide compatible
  save/restore semantics; format compatibility is a stretch goal).
- Lua scripting is a parity goal but scheduled late (§14); Redis Functions
  (FCALL) are out of scope for v1.
- **Command renaming** (Redis's `rename-command` config that disguises
  dangerous commands like `FLUSHALL` or `CONFIG` under obscure names). This is
  security by obscurity — it stops nobody who can enumerate the command table,
  and it breaks drop-in client compatibility. Real protection comes from
  authentication and ACLs (§9), not from hiding command names.

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

| Surface          | Default port | Purpose                                                                                                                                                                                                                    |
|------------------|--------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **RESP port**    | 6379         | Redis-compatible wire protocol (RESP2/RESP3, TLS optional, Unix socket optional). Drop-in replacement surface.                                                                                                             |
| **gRPC port**    | 6380         | Protobuf service API — typed, binary, no parsing. Streaming support for pub/sub and monitor-like feeds. JWT bearer auth via call metadata (§9.3).                                                                          |
| **HTTP/WS port** | 6381         | chi-based HTTP mux: login/JWT auth endpoints (§9.3), account-administration endpoints (§9.5), management/monitoring REST API (OpenAPI 3.0), `/metrics` (Prometheus), health checks, the React web UI, **and** the WebSocket endpoint for binary protobuf command access with resumable sessions (§9.4). |

Rationale for combining HTTP + WebSocket on one port: they share the chi mux
(WS upgrade handled by a chi route), one TLS stack, one middleware chain, one
auth/JWT stack, one set of ops endpoints. The RESP and gRPC ports stay dedicated
for max throughput.

---

## 4. Concurrency Architecture (the core design)

### 4.1 Sharded keyspace with owner goroutines

The keyspace is split into **N shards** (default: `4 × GOMAXPROCS`, rounded up to
a power of two; configurable). Each shard owns:

- its own hash table (pluto's native sharded hash table, §5.3 #3 — one striped
  table per keyspace; per-shard views derive from the striping),
- its own expiry min-heap (`pluto/heap_ts`),
- its own stats counters.

Routing: `shard = (crc64(key) * 0x9E3779B97F4A7C15) >> (64 - log2 N)` —
Fibonacci hashing over the pluto `crc` package's CRC-64, identical to
sharded_hash_ts's internal stripe routing, so table stripe i is exactly
shard i. The multiply is required: the raw low bits of the MSB-first CRC
are content-independent for short keys, and the raw high bits cluster for
structured keys ("key-0001"…), both of which would collapse the keyspace
onto a few shards (fixed in M3; probe in `note/crc-probe`). All commands
that touch a single key execute **inside the owning shard's goroutine**,
which serializes per-key access with **zero locks on the hot path** — the
same safety Redis gets from its single thread, but N-way parallel.

```
conn goroutine ──parse──► route by key ──► shard queue ──► shard goroutine ──► reply
```

### 4.2 Execution model per command class

| Command class                                                    | Execution                                                                                                                                                                                                                                                                                                                                                          |
|------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Single-key (GET/SET/HSET/LPUSH/…)                                | Dispatched to the key's shard goroutine; fully parallel across shards.                                                                                                                                                                                                                                                                                             |
| Multi-key same-shard (MSET/MGET/DEL when all keys hash together) | Single shard task, atomic within shard.                                                                                                                                                                                                                                                                                                                            |
| Multi-key cross-shard (MGET, DEL, EXISTS, cross-slot rename)     | Fanned out to shards, results joined by the connection goroutine. **Documented semantic**: cross-shard commands are *not* transactional across shards in v1 fast path (per-shard atomicity only), matching what clients tolerate from Cluster mode. A strict mode (two-phase shard locking, Redis-exact semantics) is available via config for DROP-IN strictness. |
| Keyspace-wide (KEYS, SCAN, DBSIZE, FLUSHALL, RANDOMKEY)          | SCAN iterates shard-by-shard with cursors `shard:inner-cursor` (pluto cursor scan, §5.3 #11); KEYS/FLUSHALL fan out.                                                                                                                                                                                                                                               |
| Pub/Sub                                                          | Dedicated broker goroutine (per-channel sharded if needed); SUBSCRIBE moves the connection into push mode. Over WebSocket, pushed messages flow through the resumable-session layer so reconnects lose nothing (§9.4).                                                                                                                                             |
| Transactions (MULTI/EXEC)                                        | All commands of a transaction are coalesced and executed with the strict cross-shard path (shard locks taken in shard-id order to avoid deadlock). WATCH uses per-shard version counters.                                                                                                                                                                          |
| Blocking (BLPOP, XREAD BLOCK, …)                                 | The *wait* never occupies a shard goroutine: the command registers interest and parks the connection goroutine; shard events wake waiters via channels.                                                                                                                                                                                                            |

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

## 5. Data Structures — pluto Mapping

pluto (`github.com/pschlump/pluto`, Go 1.27, zero deps, generics, range-over-func
iterators) rule: packages are goroutine-safe **only** if suffixed `_ts`
(internal `sync.RWMutex`; ten also expose `Lock()`/`Unlock()` + `Nl*` no-lock
methods for atomic compound ops).

**The pluto library now provides every structure Ultima needs** — all eleven
gaps originally identified for Ultima (§5.3) are implemented, including the
sharded hash table, stream structure, quicklist, LFU counters, thread-safe LRU,
and cursor-based incremental table scans.

Implementation note: the pluto structures built for Ultima (§5.3)
use `iter.Seq2[int, T]` for their index-ordered iterators in a number of places
where the original requirement documents specified other iterator shapes. Callers
should expect `Seq2` (index, value) pairs rather than plain `Seq[T]` scans.

### 5.1 Redis type → pluto structure

| Redis type             | Ultima encoding                                                               | pluto package(s)                                                                                  |
|------------------------|-------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------|
| Keyspace (per shard)   | Hash: key → `*Entry` (type tag, value ptr, expire-at, version)                | pluto's native **sharded hash table** `sharded_hash_ts` (§5.3 #3)                                 |
| String                 | `[]byte` + int-detection for INCR fast path                                   | stdlib                                                                                            |
| Hash                   | small: sorted slice or `dll` of field/value pairs; large: hash of fields      | `hash_grow_ts` / `cuckoo_ts` per-hash, or Go map under shard lock                                 |
| List                   | segmented deque (quicklist equivalent)                                        | `quicklist_ts` (§5.3 #10) — `dqueue_ts` for the small-list fast path                              |
| Set                    | small-int: sorted int slice (intset equivalent); large: hash with unit values | `hash_grow_ts`/`cuckoo_ts`                                                                        |
| Sorted set             | skiplist + hash pair (like Redis large zset)                                  | `skip_list_ts` + `hash_grow_ts` — rank/range API available (§5.3 #1)                              |
| Stream                 | append-only segmented log keyed by ID                                         | pluto `stream_ts` (§5.3 #2)                                                                       |
| Expiry (per shard)     | min-heap on expire-at ms                                                      | `heap_ts`                                                                                         |
| Eviction (LRU)         | capacity-bounded LRU                                                          | `lru_ts` (thread-safe LRU, §5.3 #6)                                                               |
| Eviction (LFU)         | approximate frequency counters                                                | `lfu` Morris counters (§5.3 #5)                                                                   |
| RDB checksums          | CRC-64                                                                        | `crc`                                                                                             |
| Glob matching for KEYS | wildcard matching                                                             | `trie.KeysThatMatch` or simple glob (Redis uses `stringmatchlen`)                                 |

### 5.2 Expiry design (replacing expire.c/ebuckets.c)

- **Passive**: every shard access checks `expireAt` before use (same as
  `expireIfNeeded`).
- **Active**: each shard goroutine runs a periodic time-boxed sweep popping its
  `heap_ts` expiry heap (replaces Redis's sampling cycle; a heap gives exact
  earliest-expiry ordering at O(log n) per expire instead of sampled buckets).
- Hash-field TTLs (Redis 7.4+/8 feature, `HEXPIRE` etc.) stored in a per-hash
  mini-heap — phase 2.

### 5.3 Gaps — all implemented in pluto

All structures originally requested from the pluto side are now **done**. Where
the implementations differ from the original requirement documents in
`docs/pluto/`, the notable difference is the use of `iter.Seq2[int, T]`
iterators (see the note in §5).

1. **Sorted-set range/rank ops on skip_list** — `Range(lo,hi)`, `Rank`, `Ceil`,
   `Floor`, by-index access; unblocks ZRANGEBYSCORE, ZRANK, ZREMRANGEBYRANK, …
   → `docs/pluto/01-skip-list-range-rank.md`
2. **Stream structure** — radix-ordered map of stream IDs → packed entry blocks,
   with consumer-group metadata; unblocks XADD/XRANGE/XREAD/XGROUP…
   → `docs/pluto/02-stream.md`
3. **Sharded concurrent hash table** (`sharded_hash_ts`) — single logical table,
   internal striping, unified SCAN cursor. This is the planned keyspace table
   (§5.1) instead of N independent `cuckoo_ts` tables.
   → `docs/pluto/03-sharded-hash-table.md`
4. **HyperLogLog** — unblocks PFADD/PFCOUNT/PFMERGE.
   → `docs/pluto/04-hyperloglog.md`
5. **LFU counter structure** (Morris-counter approx frequency) — unblocks
   `allkeys-lfu`/`volatile-lfu` eviction policies.
   → `docs/pluto/05-lfu-counter.md`
6. **Thread-safe LRU** (`lru_ts`) — the eviction LRU for the project (§13.2).
   → `docs/pluto/06-lru-ts.md`
7. **Thread-safe patricia/radix trie** (`patricia_trie_ts`).
   → `docs/pluto/07-patricia-trie-ts.md`
8. **Geo helpers** (geohash encode/decode + neighbor search on a sorted set) —
   unblocks GEOADD/GEOSEARCH…
   → `docs/pluto/08-geo.md`
9. **Bitmap/bitfield helpers**.
   → `docs/pluto/09-bitmap-bitfield.md`
10. **Bounded segmented deque** (`quicklist_ts`) — the quicklist equivalent for
    very large lists; also improves memory footprint for the list type (§5.4).
    → `docs/pluto/10-segmented-deque.md`
11. **Cursor-based incremental Scan on hash tables** — Redis-`dictScan`-style
    cursors that survive resize, implemented on `hash_grow_ts`, `cuckoo_ts`, and
    `sharded_hash_ts`; SCAN/HSCAN/SSCAN/ZSCAN need no whole-shard snapshots.
    → `docs/pluto/11-hash-table-cursor-scan.md`

### 5.4 Memory expectations

Go maps/headers cost more per key than Redis's listpack/intset encodings.
Expect ~1.5–2.5× RAM per key versus Redis for small values; large lists fare
better now that `quicklist_ts` provides packed segments. Ultima documents
`MEMORY USAGE` equivalents and offers `OBJECT ENCODING`-style introspection over
its own encodings. If footprint becomes a priority, a listpack-like packed
encoding for small hashes/sets is a later optimization (§14).

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
  web UI and client libraries) via `bin/gen.sh` (same pattern as exsms).
- **Decided (benchmarked): typed command envelope, not string-parsed commands.**
  The original sketch had a single generic `Exec(command string, args []bytes)`,
  forcing the server to re-parse the command name and every non-blob argument
  out of text. A benchmark (`note/grpc-vs-text-benchmark`, results in its
  README) measured the two designs end-to-end over real gRPC: typing saves
  ~150–280 ns/op round-trip with 3× fewer allocations (server unmarshal+exec
  is 2.4× faster for an option-bearing SET), while a unary RPC costs ~45 µs —
  so the decisive throughput lever is transport amortization (stream/batch:
  45 µs → 2.8 µs/op), with typing adding ~3.5% on top and, just as
  importantly, exact float fidelity (no `%.17g` round-trip), no
  option-grammar ambiguity, and compile-time-checked clients.
- Design: hot commands (GET/SET/DEL/INCR/HGET/HSET/LPUSH/RPUSH/LPOP/RPOP/
  SADD/ZADD/MGET/MSET/EXPIRE/… — the ~30 commands that carry >95% of
  traffic) get fully typed protobuf messages, one per command, riding in a
  `Command` envelope over a bidi stream. The generic form remains as an
  escape hatch for the long tail, so the proto surface does not have to
  model all ~250 commands (BITFIELD, XADD auto-IDs, etc.) up front:

```proto
service Ultima {
  rpc Exec(stream Command) returns (stream CommandResponse);     // pipelined, ordered, typed
  rpc ExecBatch(BatchRequest) returns (BatchResponse);           // same envelope, unary batch
  rpc ExecGeneric(CommandRequest) returns (CommandResponse);     // tooling/CLI convenience
  rpc Subscribe(SubscribeRequest) returns (stream PushEvent);    // pub/sub + keyspace notifications
  rpc Monitor(MonitorRequest) returns (stream CommandEvent);     // MONITOR equivalent
}
// Exactly one field set (protobuf oneof); dispatch is a tag switch — no strings.
message Command {
  oneof cmd {
    SetCommand   set     = 1;   // { bytes key; bytes value; int64 ttl_ms; bool nx; bool xx; bool get; }
    GetCommand   get     = 2;   // { bytes key; }
    IncrCommand  incr    = 3;   // { bytes key; int64 delta; }
    ZaddCommand  zadd    = 4;   // { bytes key; double score; bytes member; }
    // … one message per hot command …
    CommandRequest generic = 100; // escape hatch: { string command; repeated bytes args; }
  }
  uint64 db  = 101;
  uint64 seq = 102;  // client correlation id for pipelined ordering
}
```

- Every gRPC command — typed or generic — maps onto the same internal command
  executor as RESP; the typed front-end skips the parse stage entirely and
  calls the executor's typed entry points, while RESP and the generic
  envelope field go through the parser. This is the key architectural
  invariant: **one command engine, three front-ends**, with the parse shim
  optional for binary clients.
- Value representation in protobuf mirrors RESP3 types (null, int, double,
  blob-string, array, map, error) so gRPC clients get RESP3-grade fidelity.
- Unary per-command RPCs (ExecSet, ExecGet, …) may be generated for tooling
  convenience, but the stream is the high-throughput path.
- Auth: JWT access token in `authorization: bearer …` call metadata (§9.3).

### 6.3 WebSocket (binary protobuf) on the HTTP/WS port

- `gorilla/websocket` (as in exsms) at `/ws/v1` (chi route).
- Frames carry the same protobuf `Command`/`CommandResponse` envelope as the
  gRPC stream (§6.2) — typed hot commands plus the generic escape hatch, one
  frame per command, replies correlated by `seq`. A browser-friendly binary
  channel with no RESP parsing and no server-side text conversion; the parse
  savings measured in §6.2 apply here proportionally more, since a WS frame
  is cheaper than an HTTP/2 RPC. Text JSON frames optionally supported for
  debugging.
- Auth: JWT access token presented during the upgrade (§9.3); sessions are
  resumable across reconnects with no loss of pushed messages (§9.4).
- The web UI and both client libraries use this endpoint for live data (plus
  REST for CRUD and auth).

### 6.4 Operator CLIs

Each binary access protocol gets its own operator CLI binary (§14.1):
`ultima-cli` (RESP, the redis-cli analogue), `ultima-ws-cli` (WebSocket with
binary protobuf frames), and `ultima-grpc-cli` (gRPC). All three are thin
shells over the same command surface — one command engine, three front-ends,
three matching CLIs.

---

## 7. Command Coverage Plan

Redis 8.9.241 ships ~458 command-table entries (~250+ top-level commands).
Coverage is tracked in a machine-readable manifest
(`lib/commands/manifest.json` — name, group, status, since-version, notes),
generated docs page, and enforced by tests. Phased:

| Phase                                              | Groups                                                                                                    | Representative commands                                                                                                                                          |
|----------------------------------------------------|-----------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **P0 — core KV**                                   | connection, server (subset), string, keyspace, generic                                                    | PING, HELLO, AUTH, SELECT, SET/GET/DEL/EXISTS/EXPIRE/TTL/TYPE/SCAN/INCR/APPEND/GETSET/MGET/MSET, INFO, DBSIZE, FLUSHDB, CONFIG GET/SET (subset), CLIENT (subset) |
| **P1 — collections**                               | hash, list, set, sorted-set                                                                               | H*, L*, S*, Z* (pluto zset range/rank + quicklist, §5.3 #1/#10)                                                                                                  |
| **P2 — transactions & pub/sub**                    | MULTI/EXEC/DISCARD/WATCH/UNWATCH/RESET, SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/PUBLISH/PUBSUB, blocking list/zset ops (BLPOP/BRPOP/BLMPOP/BLMOVE/BRPOPLPUSH, BZPOPMIN/BZPOPMAX/BZMPOP). SSUBSCRIBE/SPUBLISH (sharded pub/sub) deferred to M8 — it exists for cluster slot routing and v1 is single-node (§15 D8); keyspace notifications (notify-keyspace-events) deferred to M5, where the expiry/eviction event sources land |                                       |
| **P3 — streams, scripting-lite, persistence cmds** | X* (pluto stream structure, §5.3 #2), SAVE/BGSAVE/BGREWRITEAOF/LASTSAVE, EVAL via `gopher-lua` (decided, §16) |                                                                                                                                                              |
| **P4 — parity tail**                               | BITOP/BITFIELD, GEO*, PF*, OBJECT, MEMORY, DEBUG (subset), hash-field TTLs (HEXPIRE…), ACL (subset), SORT |                                                                                                                                                                  |
| **P5 — superset**                                  | see §12                                                                                                   |                                                                                                                                                                  |

Full per-command semantics (error strings, arity, edge cases like
`INCR` overflow, `SET` option combinations) are validated against real Redis via
a differential test harness (§14.3).

---

## 8. Configuration

Modeled directly on exsms `lib/config/config.go`:

- JSON config file, `--cfg` flag; `default:"..."` struct tags applied via
  reflection *before* unmarshal (file overrides defaults); `$ENV$NAME`
  substitution (and `$ETCD$/key` if we want it later) via `substituteEnvRefs`.
- `Config` struct groups: `server` (ports, TLS, shard count, limits, eviction
  policy, persistence paths), `auth` (`enabled` gate, JWT Ed25519 key-pair
  file paths, token TTLs, TOTP issuer/skew, accounts-file location, bootstrap
  admin password, WS session replay-buffer bounds), `debug` (`enabled
  map[string]bool` feature flags).
- Build stamping via `bin/gen-build-stamp.sh` → `-ldflags -X
  main.GitCommit/Version/BuildDate/GitBranchName/BuildTarget`, placeholder vars
  in `cmd/ultima-server/version.go`.
- Startup-posture check (refuse unsafe configs: RESP on 0.0.0.0 with no auth +
  no TLS, etc.), same philosophy as exsms `checkStartupPosture`.
- Also accept a subset of classic `redis.conf` keys via a translation flag
  (`--redis-conf path`) to ease drop-in migration — translation layer maps to
  the JSON model.

Example sketch (persistence/eviction keys use the as-built Redis-parity
names — `maxmemory_policy`, `appendonly`, `appendfsync`, `save`, `dir`,
`dbfilename` — not the `eviction_policy`/`aof_*` names sketched earlier,
so CONFIG GET replies diff byte-exact against Redis):

```json
{
  "server": {
    "resp_addr": ":6379",
    "grpc_addr": ":6380",
    "http_addr": ":6381",
    "shard_count": 0,
    "max_memory_mb": 0,
    "maxmemory_policy": "noeviction",
    "notify_keyspace_events": "",
    "requirepass": "$ENV$ultima_password"
  },
  "persist": {
    "dir": "./data",
    "dbfilename": "dump.rdb",
    "appenddirname": "appendonlydir",
    "appendonly": true,
    "appendfsync": "everysec",
    "save": "3600 1 300 100 60 10000",
    "snapshot_compress": false
  },
  "auth": {
    "enabled": false,
    "jwt_private_key_file": "./keys/ultima-jwt.pem",
    "jwt_public_key_file": "./keys/ultima-jwt.pub",
    "access_token_ttl": "15m",
    "refresh_token_ttl": "720h",
    "totp_issuer": "Ultima",
    "totp_skew": 1,
    "accounts_file": "",
    "bootstrap_admin_password": "$ENV$ultima_admin_password",
    "ws_replay_buffer_ms": 30000,
    "ws_replay_buffer_max_msgs": 10000
  },
  "debug": { "enabled": { "dump.commands": false } }
}
```

---

## 9. Authentication, Accounts & Login

The HTTP login facility sits **on top of the existing Ultima user/ACL system**
(the Redis-clone `ACL` users, passwords, and permissions). It adds a real
account layer for the HTTP, WebSocket, and gRPC surfaces: multiple
administrative accounts, optional TOTP two-factor authentication, and JWT
bearer tokens with refresh.

### 9.1 Account model

- Two account classes over the same user/ACL base:
  - **Administrative accounts** — manage the server and other accounts via the
    HTTP API and web UI. Ultima no longer has a single built-in admin: the
    built-in `admin` account exists only as a bootstrap identity and can
    **create additional administrative accounts**, each with a username,
    password, and (optional) TOTP 2FA key.
  - **Data users** — ordinary ACL users that run commands over RESP, gRPC, and
    WebSocket, subject to their ACL permissions.
- Passwords are stored with a modern KDF (bcrypt/argon2id), never in
  plaintext; the ACL `requirepass`/`user` cleartext style is accepted only
  for Redis-compatible RESP AUTH, not for the HTTP login surface.

### 9.2 TOTP two-factor authentication

- Optional per account. When enabled, login requires the current 6-digit TOTP
  code in addition to username + password.
- Implemented with **`github.com/pschlump/htotp`** (at `../htotp`): RFC 6238
  TOTP validation (with configurable skew), cryptographically secure secret
  generation, `otpauth://` provisioning URIs, and QR-code generation so
  enrollment works with Google Authenticator and compatible apps directly
  from the web UI.
- Each account's TOTP secret can be **regenerated** (§9.5): the old secret is
  invalidated immediately and the new secret is shown once as a provisioning
  URI/QR code for re-enrollment.

### 9.3 Login and tokens (JWT)

- `POST /api/v1/auth/login` — `{ username, password, totp? }` →
  `{ access_token, refresh_token }`.
- **Access token**: short-lived signed JWT (default 15 min), carries user id,
  account class, and ACL identity. Accepted as:
  - `Authorization: Bearer …` on the HTTP management API,
  - the bearer credential on the WebSocket upgrade (query param or subprotocol
    header, since browsers cannot set headers on WS),
  - `authorization: bearer …` call metadata on gRPC.
- **Refresh token**: long-lived (default 30 days), single-use with rotation:
  `POST /api/v1/auth/refresh` returns a new access token **and** a new refresh
  token, invalidating the old one (theft detection: reuse of a rotated token
  revokes the whole token family).
- `POST /api/v1/auth/logout` revokes the refresh-token family; administrators
  can revoke all sessions of any account.
- Signing: **Ed25519 (EdDSA)** via `github.com/golang-jwt/jwt/v5`. The key
  pair comes from config file paths (§8): `auth.jwt_private_key_file`
  (PKCS#8 PEM, used to sign at login/refresh) and `auth.jwt_public_key_file`
  (PKIX/SPKI PEM, used to verify). **Both paths are required** when the auth
  system is enabled — startup fails with a clear error if either file is
  missing or unparseable; there is no shared-secret mode and no
  auto-generation (an accidentally regenerated key pair would silently
  invalidate every outstanding token). Key rotation is an operator action:
  generate a new pair, roll the files out, restart.

### 9.4 WebSocket connection recovery (no message loss on reconnect)

WebSocket connections — especially from browsers on mobile or flaky networks —
drop often. Ultima makes WS sessions **resumable**:

- Every authenticated WS connection negotiates a **session id** at connect
  time (`session` field in the protobuf handshake frame).
- The server keeps a per-session **replay buffer**: every pushed message
  (pub/sub deliveries, keyspace notifications, monitor events) is stamped with
  a monotonically increasing per-session `push_seq` and retained for a bounded
  window (`auth.ws_replay_buffer_ms` / `ws_replay_buffer_max_msgs`, §8).
- On reconnect, the client presents its session id and the last `push_seq` it
  received. If the session is still within its retention window, the server
  **replays all buffered pushes after that sequence number**, re-attaches the
  session's subscriptions (SUBSCRIBE/PSUBSCRIBE state), and resumes live
  delivery — the client observes no gap.
- If the session has expired (buffer window exceeded), the server answers with
  a `SESSION_EXPIRED` frame; the client library then re-authenticates,
  re-subscribes, and (for channels where gaps matter) re-reads current state
  via a normal command before resuming. This fallback is explicit, never
  silent.
- Command *replies* are not replayed — in-flight commands at drop time are
  reported as `ABORTED` by `seq` so the client can retry them idempotently.
- The TypeScript/JavaScript client libraries (§11) implement this handshake
  transparently: applications see a continuous event stream across reconnects.

### 9.5 Account management

HTTP endpoints (also exposed in the web UI, §10.2):

- `POST /api/v1/auth/password` — change own password (requires current
  password, plus TOTP code when 2FA is enabled). Invalidates all existing
  refresh tokens for the account.
- `POST /api/v1/auth/totp/enable` — generate a new TOTP secret; response
  carries the provisioning URI + QR code (htotp); confirmed by submitting a
  valid code from the enrolled app (`POST /api/v1/auth/totp/confirm`).
- `POST /api/v1/auth/totp/regenerate` — replace the secret (same flow as
  enable; old secret invalidated immediately).
- `POST /api/v1/auth/totp/disable` — turn off 2FA (requires password + current
  TOTP code).
- Admin endpoints (require an administrative account):
  - `GET /api/v1/admin/users`, `POST /api/v1/admin/users` (create admin or
    data user with username/password/optional TOTP key),
    `PUT /api/v1/admin/users/{name}`, `DELETE /api/v1/admin/users/{name}`,
    `POST /api/v1/admin/users/{name}/revoke-sessions`.
- The built-in `admin` account can be disabled via config once at least one
  other administrative account exists (startup-posture check warns if it is
  the only admin and still enabled).

---

## 10. HTTP Management API + Web UI

### 10.1 HTTP API (chi + OpenAPI 3.0 + validator)

Following exsms conventions:

- `api/openapi.yaml` is the contract source of truth; embedded and served at
  `/api/openapi.yaml`; Swagger UI served from embedded assets
  (`lib/httpapi`). Routes use oapi-codegen's generated `chi-server` bindings,
  wired properly (decided, §16 — unlike exsms, which generates the bindings but
  hand-registers).
- Request DTOs validated with `go-playground/validator/v10`, decoded through a
  `JsonBody` helper that reuses the config `SetDefaults` `default:`-tag
  mechanism (exsms `lib/utils/jsonbody.go` pattern).
- Middleware: request-id → request logging (`slog` JSON) → real-IP (trusted
  proxies only) → Recoverer → Timeout → Prometheus (`chiprom`-style) → JWT auth
  (§9.3) on everything except `/health`, `/ready`, and `/api/v1/auth/login`.
- Endpoint groups (all under `/api/v1`):
  - `GET /health`, `GET /ready`
  - `GET /metrics` (Prometheus, IP-allowlist guarded)
  - `/api/v1/auth/*` — login, refresh, logout, password change, TOTP
    enrollment/regeneration (§9.3, §9.5)
  - `/api/v1/admin/users*` — administrative account management (§9.5)
  - `GET /api/v1/info` — INFO as structured JSON
  - `GET /api/v1/shards` — per-shard key counts, ops/sec, queue depth, expiry
    heap size, memory
  - `GET /api/v1/clients`, `POST /api/v1/clients/{id}/kill`
  - `GET /api/v1/slowlog`, `GET /api/v1/latency`
  - `GET|PUT /api/v1/config` — runtime config (CONFIG GET/SET equivalents)
  - `POST /api/v1/flushdb`, `POST /api/v1/save`, `POST /api/v1/bgsave`
  - `GET /api/v1/keys/scan?cursor=…&match=…&count=…`, `GET /api/v1/key/{key}`
    (typed value preview), `DELETE /api/v1/key/{key}`
- Auth: JWT bearer tokens from the login facility (§9) — this *replaces* the
  earlier standalone bearer-token/mTLS sketch; RESP AUTH stays independent for
  drop-in compatibility.

### 10.2 Web UI (`./web`, React + bun + vite)

- Vite + React + TypeScript; bun for package management and scripts
  (`bun install`, `bun run dev`, `bun run build` → `web/dist`, embedded into the
  Go binary via `//go:embed` for production serving).
- Login screen against `/api/v1/auth/login` (username/password/TOTP), token
  refresh handled automatically.
- Screens: dashboard (ops/sec, memory, hit ratio, per-shard heatmap), key
  browser (scan + typed value viewer/editor), live monitor (over WS), slowlog,
  config editor, pub/sub inspector, account administration (users, 2FA
  enrollment with QR codes).
- **Interactive console**: a CLI-in-the-browser screen. A command input window
  accepts any Ultima command (same syntax as `ultima-cli`); results render in a
  scrolling output area at the bottom of the screen, newest last, with
  type-aware formatting (hashes as tables, zsets with scores, errors in red).
  The console runs over `/ws/v1`, so **server-pushed content works too**:
  `SUBSCRIBE` / `PSUBSCRIBE` put the console into push mode and incoming
  messages stream into the output area live. Thanks to session recovery
  (§9.4), a console that loses its connection replays what it missed instead of
  silently dropping messages.
- Communicates via the REST API + `/ws/v1` binary protobuf (ts-proto generated
  clients in `web/src/proto`, same gen pipeline as exsms), built on the
  TypeScript client library (§11.2) — the web UI is its first consumer and
  reference application.

---

## 11. Client Libraries & Example Applications

Client-side support ships as three official libraries — Go, TypeScript, and
JavaScript — plus a set of complete example applications that double as
documentation.

### 11.1 Go library (`clients/go`)

- The native client for Go programs and the foundation of the operator CLIs:
  `ultima-cli`, `ultima-ws-cli`, and `ultima-grpc-cli` (§6.4) are all thin
  shells over this library, so the CLI feature set and the library API stay in
  lockstep.
- Covers all three access surfaces behind one client type:
  - **gRPC client** (the high-throughput path): the typed `Command` envelope
    over the bidi stream, with typed per-command helpers
    (`client.Set(ctx, …)`, `client.ZAdd(ctx, …)`, …) plus a generic `Exec`
    escape hatch; `ExecBatch` support; `Subscribe`/`Monitor` streams.
  - **WebSocket client** for `/ws/v1` with the same typed API, including
    **connection recovery** (§9.4): automatic reconnect with backoff, session
    resume with `push_seq` replay, transparent re-subscription.
  - **HTTP/REST client** for `/api/v1/*` (auth, admin, key browsing, info).
- Token management shared across surfaces: login with
  username/password/TOTP, automatic access-token refresh, re-login on refresh
  failure.
- Built on the generated `gen/go` protobuf bindings; idiomatic Go
  (context-aware, `iter.Seq2` for scans where useful).

### 11.2 TypeScript library (`clients/typescript`)

- The primary client library, published as an npm/bun package (e.g.
  `@ultima/client`). Fully typed, with generated protobuf bindings (ts-proto,
  same `gen/ts` pipeline as the web UI).
- Covers both surfaces:
  - **REST client** for `/api/v1/*` (auth, admin, key browsing, info).
  - **WebSocket client** for `/ws/v1`: the typed `Command` envelope, typed
    per-command helpers (`client.set()`, `client.zadd()`, …) plus a generic
    `exec()` escape hatch, and a subscription API (`client.subscribe(channel,
    handler)`).
- Built-in token management: login with username/password/TOTP, automatic
  access-token refresh before expiry, re-login on refresh failure.
- Built-in **connection recovery** (§9.4): automatic reconnect with backoff,
  session resume with `push_seq` replay, transparent re-subscription, and an
  explicit `gap` event if a session expired unrecoverably.
- Framework-agnostic core (works in browsers, Node, and bun); thin React hooks
  package (`useUltima`, `useSubscription`) for UI work.

### 11.3 JavaScript library (`clients/javascript`)

- A plain-JavaScript distribution of the same client for projects without a
  TypeScript toolchain: compiled ESM + CJS builds of the TypeScript library
  with bundled `.d.ts` files, usable directly from a `<script type="module">`
  tag, Node, or bun with no build step.
- Same API surface, same recovery behavior; documented separately so JS users
  never need to read TypeScript.

### 11.4 Example applications (`examples/`, documented)

Each example is a complete, runnable application built on the client libraries
and a chapter of the library documentation. The browser examples (1–5) use the
TypeScript/JavaScript library; each also ships with a small Go variant or
companion tool (score submitter, chat bot, load generator) built on the Go
library (§11.1):

1. **Real-time game leaderboard** — players submit scores; a `ZADD`-backed
   leaderboard updates every connected browser live via pub/sub over the
   recovered WebSocket session. Demonstrates sorted sets, subscriptions, and
   reconnect-without-loss.
2. **Live chat room** — channels as pub/sub topics, history in lists, presence
   via keyspace notifications with expiring keys.
3. **Market/price ticker dashboard** — high-rate simulated price updates
   streamed to a charting UI; demonstrates backpressure and the batch API.
4. **Collaborative counter / presence board** — many clients mutating shared
   state (INCR, hashes) with live divergence-free views.
5. **Log tail viewer** — `XADD`-produced streams consumed with `XREAD` over
   WS, rendered as a tailing log console.

---

## 12. Superset Features (beyond Redis parity)

Planned after P4, each behind config flags:

1. **gRPC/WS binary protocol** (already core — §6.2/§6.3).
2. **Per-shard metrics & heatmaps** — impossible in stock Redis.
3. **Batch API** (`ExecBatch`) with server-side pipelining semantics.
4. **Keyspace change feeds** (durable-ish CDC stream over WS/gRPC — richer than
   keyspace notifications; rides on the resumable-session machinery, §9.4).
5. **Multi-pattern SCAN** and server-side Lua-free computed transforms.
6. **Pluggable persistence backends** (§13.3) — including pluto
   `b_tree_disk_ts`-backed warm tier for overflow (values are `uint64` there, so
   it stores an indirection to blob files; experimental).

---

## 13. Persistence, Eviction, and Ops Semantics

### 13.1 Persistence

- **Snapshot (RDB-equivalent)**: per-shard consistent snapshot via shard-goroutine
  pause-and-drain (milliseconds per shard, staggered — no fork needed in Go),
  serialized in our own compact format with `crc` CRC-64 checksums; LZF or
  zstd compression. Optionally emit Redis-compatible RDB later (stretch).
- **AOF-equivalent**: per-shard append logs + manifest (MP-AOF-like), fsync
  policies `always/everysec/no` on a background goroutine (bio replacement);
  rewrite = compact replay into new logs.
- Restore on startup, `SAVE`/`BGSAVE`/`BGREWRITEAOF`/`LASTSAVE` commands, plus
  HTTP triggers.

### 13.2 Eviction

- `maxmemory` with policies `noeviction`, `allkeys-lru` (pluto thread-safe LRU,
  §5.3 #6), `volatile-lru`, `volatile-ttl` (expiry heap order), `allkeys-random`,
  `volatile-random`; LFU policies (`allkeys-lfu`, `volatile-lfu`) use the pluto
  LFU counter structure (§5.3 #5).
- **Decided (§16): exact LRU via pluto's LRU** rather than Redis-style sampled
  LRU — under the owner-goroutine model per-shard exact LRU is cheap enough; the
  M5 benchmark is now a confirmation rather than a selection gate.

### 13.3 Databases

Multiple logical DBs (SELECT 0..15) supported via per-DB shard sets; FLUSHDB is
per-DB. (Configurable max DBs; default 16 like Redis.)

---

## 14. Project Layout, Tooling, Milestones

### 14.1 Layout (modeled on exsms)

```
ultima/
├── cmd/
│   ├── ultima-server/        # main binary (main.go, version.go, startup.go, router.go)
│   ├── ultima-bench/         # benchmark driver
│   ├── ultima-cli/           # operator CLI over RESP (later)
│   ├── ultima-ws-cli/        # operator CLI over the WebSocket/protobuf endpoint (later)
│   └── ultima-grpc-cli/      # operator CLI over the gRPC endpoint (later)
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
│   ├── auth/                 # accounts, JWT issue/verify/refresh, TOTP (htotp), middleware
│   ├── wssession/            # WS resumable sessions, replay buffers (§9.4)
│   ├── grpcsrv/              # gRPC front-end
│   ├── wssrv/                # WebSocket front-end
│   ├── handler/              # HTTP API (chi), embedded openapi.yaml
│   ├── metrics/              # Prometheus
│   └── reqlog/ utils/        # middleware, JsonBody helpers
├── clients/
│   ├── go/                   # Go client for gRPC + WS + REST; basis of the CLIs (§11.1)
│   ├── typescript/           # @ultima/client — TS client for HTTP + WS (§11.2)
│   └── javascript/           # plain-JS ESM/CJS distribution (§11.3)
├── examples/                 # leaderboard, chat, ticker, presence, log-tail apps (§11.4)
├── api/openapi.yaml          # HTTP contract (+ oapi-codegen.yaml config)
├── proto/ultima/v1/          # protobuf IDL
├── gen/go, gen/ts            # generated protobuf code
├── web/                      # React + bun + vite management UI (incl. console)
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
`pschlump/pluto`, `pschlump/htotp` (TOTP 2FA), `golang-jwt/jwt/v5`,
`go.uber.org/goleak` (tests). Go 1.27.

### 14.2 Build tooling (Makefile, mirroring exsms)

- `make gen_proto` (bin/gen.sh → protoc Go+TS, copy into web/src and
  clients/typescript/src)
- `make generate` (oapi-codegen chi-server bindings) / `make sync-openapi` (embed copy +
  drift check, prerequisite of server builds)
- `make build` (build stamp + all binaries), `make run` (dev config in tests/),
  `make test` (`go test ./...`, integration in `tests/`), `make lint`
  (golangci-lint v2), `make tidy`, `make clean`
- `make clients` — build the Go, TypeScript, and JavaScript client packages and
  the example apps
- `make bench` — redis-benchmark + memtier suite, results committed to
  `docs/benchmarks/`

### 14.3 Testing strategy

1. **Unit tests** colocated (`lib/.../*_test.go`), goleak for goroutine leaks.
2. **Integration tests** in `tests/`: real client (go-redis) against a spawned
   server; auth flow tests (login → refresh rotation → revocation), TOTP tests
   against htotp's RFC 6238 vectors, and WS recovery tests (kill connection
   mid-stream, assert zero lost pushes on resume).
3. **Differential testing**: harness runs the same command sequences against
   Ultima and real Redis 8.x (from `note/redis`, built locally) and diffs
   replies, including error strings — the primary parity gate. Redis's own TCL
   suite (`note/redis/tests/`) is reused where feasible by pointing it at the
   Ultima port.
4. **Concurrency stress**: randomized multi-client workloads under `-race`,
   invariant checkers (linearizability spot checks per key).
5. **Benchmarks**: `redis-benchmark -P/-c` sweeps vs local Redis, per milestone;
   memtier for mixed workloads; pprof profiles attached to reports.

### 14.4 Milestones

| MS     | Deliverable                                                                                                                 | Exit criteria                                                                                      |
|--------|-----------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| **M0** | Skeleton: config, logging, three listeners (stub), Makefile, lint, CI-local                                                 | `make build test lint` green; PING on all three surfaces                                           |
| **M1** | Shard engine + RESP front-end + P0 commands; redcon fork w/ RESP3                                                           | redis-cli fully works for P0; differential harness green on P0; first benchmark report vs Redis    |
| **M2** | P1 collections (pluto zset range/rank + quicklist, §5.3 #1/#10)                                                             | differential green on H/L/S/Z                                                                      |
| **M3** | P2 transactions + classic pub/sub + blocking list/zset ops (SSUBSCRIBE → M8; keyspace notifications → M5)                   | MULTI/EXEC + WATCH stress green; redis-benchmark pub/sub                                           |
| **M4** | gRPC + WS front-ends; proto IDL stable                                                                                      | go + ts clients round-trip; parity with RESP replies                                               |
| **M5** | Expiry hardening, eviction, persistence (snapshot + AOF), keyspace notifications (notify-keyspace-events; deferred from P2) | crash-recovery tests; maxmemory soak                                                               |
| **M6** | Auth system (§9) + HTTP API + web UI v1. **M6a done** (accounts, Ed25519 JWT, TOTP, auth on HTTP/gRPC/WS — `docs/m6-detailed-plan.md`); **M6b done** (resumable WS sessions, §9.4 — `lib/wssession`) | login/refresh/TOTP green; multi-admin accounts; WS recovery tests pass; dashboard + console live   |
| **M7** | Client libraries (§11) + example applications                                                                               | Go + TS + JS packages build; CLIs run on the Go client; leaderboard + chat examples run end-to-end |
| **M8** | P3/P4 parity tail (streams, Lua-lite, bitfield, geo, PF\*), sharded pub/sub SSUBSCRIBE/SPUBLISH (deferred from P2)          | differential green on covered tail                                                                 |
| **M9** | Superset features (§12) + performance campaign                                                                              | ≥4× Redis on target workload; final report                                                         |

Status: M0–M5 are done (M5 completed via the M5a–M5d phases of
`docs/m5-detailed-plan.md`; soak report in `docs/benchmarks/M5-*.md`,
implementation memo in `note/M5-implemented.md`).

---

## 15. Key Decisions (summary)

| #   | Decision                                                                              | Rationale                                                                                                                                |
|-----|---------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| D1  | Goroutine-per-connection + sharded keyspace with owner goroutines (no hot-path locks) | Directly replaces ae.c/single-thread with N-way parallelism; per-key serialization preserved                                             |
| D2  | Vendor & extend `tidwall/redcon` as `lib/resp`, add RESP3                             | Proven RESP2 server faster than Redis in benchmarks; small codebase; MIT                                                                 |
| D3  | One command engine, three front-ends (RESP/gRPC/WS)                                   | No logic duplication; parity bugs fixed once                                                                                             |
| D4  | pluto `_ts` structures inside owner-goroutine shards (locks mostly uncontended)       | Reuse tested generics; `Lock`/`Nl*` available for compound ops                                                                           |
| D5  | Heap-based exact expiry per shard instead of sampled active expiry                    | Simpler + exact; `heap_ts` exists today                                                                                                  |
| D6  | Config = JSON + `default:` tags + `$ENV$` (exsms pattern)                             | Consistency with existing tooling; reflection machinery proven                                                                           |
| D7  | HTTP API contract-first (`api/openapi.yaml`, chi, validator)                          | exsms pattern; doc-drift tests                                                                                                           |
| D8  | No Redis Cluster/Sentinel in v1; single-node, many cores                              | Focus throughput goal; scale-out later                                                                                                   |
| D9  | Own snapshot/AOF format first, RDB-compat later                                       | Ship persistence early without format reverse-engineering                                                                                |
| D10 | Memory footprint parity explicitly traded for throughput                              | Go encodings cost more RAM; documented; mitigated for lists by `quicklist_ts`                                                            |
| D11 | Wire oapi-codegen `chi-server` bindings properly (not exsms-style hand-registration)  | Resolved open question #1                                                                                                                |
| D12 | `gopher-lua` for EVAL scripting (pure Go, no CGo)                                     | Resolved open question #3                                                                                                                |
| D13 | Keyspace on pluto's native sharded hash table (not N independent `cuckoo_ts`)         | Resolved open questions #2/#5; unified SCAN cursor, internal striping                                                                    |
| D14 | Exact LRU eviction via pluto's thread-safe LRU                                        | Resolved open question #4                                                                                                                |
| D15 | Typed command `oneof` envelope over bidi stream for gRPC/WS, generic escape hatch     | Benchmarked (`note/grpc-vs-text-benchmark`): kills all text parse, float-exact, 3× fewer allocs; throughput lever is stream amortization |
| D16 | JWT access + refresh tokens (with rotation) for HTTP/WS/gRPC auth                     | Standard stateless auth for browser clients; refresh rotation limits token-theft window; one credential works on all three surfaces      |
| D17 | Multiple administrative accounts on the user/ACL base; optional TOTP 2FA via `pschlump/htotp` | Single built-in admin is not operable; htotp gives RFC-tested TOTP, provisioning URIs, and QR enrollment with zero new deps    |
| D18 | Resumable WebSocket sessions: per-session replay buffer + `push_seq` handshake        | Browser/mobile WS connections drop routinely; replay-on-reconnect guarantees no lost pushes without client-visible gaps                  |
| D19 | Official Go + TypeScript + JavaScript client libraries with full example applications   | The access surfaces need first-class client support; the Go library is the shared basis of the operator CLIs; examples (leaderboard, chat, …) are the documentation that proves the API |
| D20 | JWT signing is Ed25519 (EdDSA) via `golang-jwt/jwt/v5`; key pair read from config file paths (`jwt_private_key_file` + `jwt_public_key_file`, both required) | Asymmetric keys keep any shared secret out of config; verification needs only the public key; no auto-generation — a regenerated pair silently invalidates all tokens; rotation = file rollout + restart |

## 16. Open Questions — Resolved

All five open questions from v0.1 are resolved (answers from `p0.md`):

1. oapi-codegen: wire generated chi-server bindings properly, or hand-register
   routes as exsms does? → **Wire chi-server bindings properly.** (§10.1)
2. Strict vs fast cross-shard multi-key semantics as default? → **Pluto now
   implements a sharded hash table** (§5.3 #3); the keyspace builds on it. The
   fast-default/strict-flag behavior of §4.2 stands for cross-shard fan-out
   commands.
3. Lua engine for EVAL: `gopher-lua` vs defer scripting? → **`gopher-lua`.**
   (§7 P3)
4. Exact-LRU vs sampled-LRU eviction? → **Pluto now implements an LRU for this
   project** (§5.3 #6); exact LRU it is. (§13.2)
5. Native sharded table from pluto vs N independent `cuckoo_ts`? → **Pluto now
   implements a sharded hash table** (§5.3 #3); use it. (§5.1)
