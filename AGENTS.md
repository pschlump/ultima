# AGENTS.md

Guidance for AI coding agents working in this repository. Assumes no prior
knowledge of the project.

## Project Overview

**Ultima** is a superset clone of Redis written in Go: a drop-in replacement
for the Redis wire protocol and command set, plus gRPC and
WebSocket/protobuf front-ends and a web management UI. The driving goal is
**substantially higher throughput than Redis** by replacing Redis's
single-threaded command execution with a parallel, goroutine-based
architecture.

The authoritative reference is `docs/ULTIMA-DESIGN.md` (cited in code as
`§N.N`); it records settled decisions (§15, D1–D20) that must not be
silently reversed. Milestones M0–M9 are defined in §14.4. **M0–M8 are
done** (M8e included); the milestone-by-milestone narrative and per-
milestone architecture journals live in `docs/implementation-history.md` —
consult it when working in an area whose design was settled by an earlier
milestone (transaction/pause model M3, eviction M5b, persistence M5c,
auth/WS sessions M6b, scripting M8, VM pool M8e).

## Technology Stack

- **Go 1.27** (module `github.com/pschlump/ultima`). **No CGo** — enforced
  by `CGO_ENABLED=0 go build ./...`; no Docker/CI config in the repo.
- Sibling checkouts pulled in via `replace` directives — they must exist
  for the module to build:
  - `../pluto` — `github.com/pschlump/pluto`, generic data-structure
    library (thread-safe `_ts` variants); per-structure specs in
    `docs/pluto/`. **Standing permission**: when the efficient way to
    implement something is to add a feature to `../pluto` (e.g. `lru_ts`),
    add it there — with tests — rather than working around its API here,
    and update the matching `docs/pluto/` spec. The same applies to
    `../gopher-lua`'s `host` package for scripting needs.
  - `../htotp` — `github.com/pschlump/htotp`, TOTP 2FA (D17).
  - `../gopher-lua` — `github.com/pschlump/gopher-lua`, M8 Lua scripting
    (D12): the `host` package (wazero runtime + SHA-pinned
    `lua51_prod.wasm` blob; scripts compile to wasm).
- Other key deps (`go.mod`): `go-chi/chi/v5` (HTTP router),
  `gorilla/websocket`, `google.golang.org/grpc` + `protobuf`,
  `go.uber.org/goleak` (leak detection in tests), `golang-jwt/jwt/v5` +
  `golang.org/x/crypto` (Ed25519 EdDSA JWTs per D20, bcrypt).
- `note/` is scratch/reference (incl. a Redis source checkout under
  `note/redis/`); gitignored, lint-excluded.

## Runtime Architecture

One binary (`ultima-server`), one process, **three network surfaces**
(§3), all wired in `cmd/ultima-server/startup.go`:

| Surface        | Default addr | Implementation                          |
|----------------|--------------|------------------------------------------|
| RESP (Redis protocol) | `:6379` | `lib/resp` (vendored redcon fork) → `lib/commands.Engine` |
| gRPC           | `:6380`      | `lib/grpcsrv` on generated `gen/go/ultima/v1` code; server reflection on |
| HTTP/WebSocket | `:6381`      | `lib/httpapi` (management API) + `lib/wssrv` on a chi mux (`cmd/ultima-server/router.go`) |

Request flow: each front-end parses its wire format, calls
`commands.Engine.Execute(ConnState, args)`, and renders the returned
`resp.Value` (D3: one command engine, three front-ends).

- **Sharding** (`lib/shard`): each logical DB lives in one pluto
  `sharded_hash_ts.ShardedHash` whose stripe count equals the shard count;
  key → shard routing is Fibonacci hashing,
  `(crc64(key)*0x9E3779B97F4A7C15) >> (64-log2 N)` — identical to the
  table's internal stripe routing, so stripe i is shard i (raw CRC bits
  cluster for structured keys; do not "simplify" this). Each shard has an
  owner goroutine draining a task queue (`Engine.Do`/`DoMulti`/`DoShard`);
  all data-mutating work runs inside the owning shard's goroutine — no
  caller-side locks on the hot path. `DoMulti` fan-out closures run
  concurrently — shared writes must be synchronized or slot-indexed.
  Pipelined RESP bursts are coalesced (M9b): `commands.Engine.ExecuteBurst`
  (`lib/commands/burst.go`) groups whitelisted single-key segments into
  per-shard tasks via `shard.DoTokAsync` (pooled completion channels) and
  `do()` runs inline through a per-shard `ConnState.inls` slot; coalescing
  is disabled with maxmemory configured (EvictNow self-deadlock) and in
  MULTI. Extend `burstInlineable` only with handlers verified to use
  `e.do` exclusively (no doMulti/PauseAll/blocking/ConnState mutation).
- **Cross-shard atomicity**: `shard.Engine.PauseAll` parks every shard
  goroutine; only token-carrying tasks run. EXEC and EVAL take it (S3);
  fan-out tasks issued under a pause must carry the caller's token
  (`shard.FlushDBTok/FlushAllTok/DBSizeTok`) or they deadlock.
- **Expiry**: passive on access plus an exact per-shard min-heap
  (`heap_ts`) swept periodically by the shard goroutine (D5), time-boxed
  with adaptive cadence.
- **Blocking ops**: waiters register in the owning shard's FIFO registry;
  the connection goroutine parks on a channel — never a shard goroutine.
- **Pub/sub**: `lib/pubsub` broker, delivery on the publisher's goroutine
  into per-connection bounded push queues; full queue = slow consumer →
  connection closed.
- **Persistence** (`lib/persist`, D9 own formats): per-(db,shard) snapshot
  segments + per-shard seq-stamped AOF with global sequence merge at
  replay; restore-before-serve.
- **Scripting** (`lib/scripting`, D12): gopher-lua wasm host; EVAL runs
  under PauseAll; VMs come from the per-script pool (`pool.go` — guest GC
  is stopped, so pooled VMs are recycled on run count / heap watermark /
  fatal errors / SCRIPT FLUSH; knobs `script_vm_pool_size` (0 disables),
  `script_vm_pool_max`, `script_vm_recycle_runs`, `script_vm_recycle_pct`).
- Shard count: config `shard_count`, `0` = 4×GOMAXPROCS, rounded up to a
  power of two (`shard.ResolveShardCount`).
- Graceful shutdown: SIGINT/SIGTERM → drain gRPC, close HTTP and RESP,
  persist close, stop shard goroutines (10 s cap).

**Commands**: the registry is `lib/commands/table.go`. Coverage: P0
(connection/strings/keyspace/server), P1 (hash/list/set/zset incl. scans),
P2 (transactions, classic pub/sub, blocking list/zset ops), keyspace
notifications (M5a), MONITOR (M6d), Lua scripting (EVAL*/SCRIPT, M8), and
the M8 tail (streams, bitfield, geo, PF*). Ultima reports Redis
compatibility version 7.2.7 (`commands.CompatVersion`).

## Code Organization

```
cmd/ultima-server/   main binary: main.go, startup.go (wiring), router.go (chi), version.go (build stamp)
cmd/ultima-cli/      operator CLIs (§6.4) — RESP, WebSocket, gRPC; thin shells
cmd/ultima-ws-cli/   over clients/go/ultima (one-shot + REPL, push streaming).
cmd/ultima-grpc-cli/ Terminal REPLs use ergochat/readline: tab completion, the
                     client-side `help` command, and vi/emacs modes via `\mode`
clients/go/ultima/   Go client library (§11.1): RESP/gRPC/WS/REST behind one umbrella
                     Client, typed helpers, §9.4 session recovery on WS, token manager.
                     Also the shared CLI REPL machinery: repl.go (scanner REPL),
                     repl_readline.go (readline REPL), cmdhelp.go + generated
                     cmdhelp_data.go (bin/gen-cli-help.py: table.go + Redis 7.2.7
                     command JSON → help/completion data)
clients/typescript/  @ultima/client (§11.2): framework-agnostic TS client (WS + REST +
                     auth); the web UI consumes it via a file: dep
clients/javascript/  plain-JS ESM+CJS distribution of @ultima/client (§11.3)
examples/            leaderboard + chat example apps (§11.4), e2e-tested in tests/m7_examples_test.go
lib/config/          JSON config: `default:"..."` struct tags via reflection + `$ENV$NAME` substitution (D6)
lib/resp/            vendored + extended fork of tidwall/redcon v1.6.4 (D2); RESP3 emitters,
                     per-connection protocol versioning; kept close to upstream — excluded from lint
lib/respserver/      shared RESP front-end wiring (§6.1, D3); used by cmd and both test harnesses
lib/shard/           sharded keyspace engine, owner goroutines, routing, expiry heap,
                     WATCH tracking, PauseAll, blocking-waiter registry; evict.go (maxmemory)
lib/types/           collection value types (Hash/List/Set/ZSet, Redis-like promotion
                     thresholds); memusage.go: O(1) per-type memory estimators
lib/pubsub/          classic pub/sub broker
lib/commands/        front-end-agnostic command engine; table.go is the registry
                     (def(name, arity, flags, first, last, step, group, handler));
                     coll.go shared parsing helpers; tx.go, pubsub.go, block.go,
                     notify.go (keyspace notifications), aof.go (capture/rewrite),
                     burst.go (M9b pipelined-burst coalescing),
                     persist.go (SAVE family), monitor.go, eval.go + script.go (M8)
lib/envelope/        protobuf Command → engine argv, resp.Value ↔ protobuf Value (D3/D15)
lib/grpcsrv/         gRPC front-end: Exec bidi stream, ExecBatch, ExecGeneric, Ping,
                     Subscribe push stream, Monitor stream
lib/wssrv/           WebSocket front-end (§6.3): binary protobuf frames at /ws/v1,
                     seq-correlated replies, pub/sub pushes as seq-0 frames
lib/wssession/       resumable WS sessions (§9.4, D18): replay buffer, atomic
                     attach/takeover, retention expiry
lib/handler/         shared HTTP middleware: RequestLogger (slog) + statusRecorder
lib/httpapi/         HTTP management API (§10.1, D7/D11) implementing gen/httpapi;
                     jsonbody.go (decode → SetDefaults → validator/v10), metrics.go
                     (dedicated prometheus.Registry + metrics_allow IP guard),
                     pprof.go (M9a /debug/pprof/*, mounted by server.pprof_enabled,
                     metrics_allow + JWT-guarded),
                     swagger.go (embedded openapi.yaml + Swagger UI)
lib/auth/            auth core (§9, D17/D20): Ed25519 JWTs, bcrypt account store with
                     refresh-token rotation/theft detection, TOTP, middleware +
                     gRPC interceptors; gated by auth.enabled
lib/persist/         persistence (§13.1, D9): format.go (snapshot codec), snapshot.go,
                     aof.go (per-shard logs + BGREWRITEAOF), replay.go (seq merge),
                     manager.go (save rules, fsync policies, restore-before-serve)
lib/scripting/       Lua scripting (§7 P3, D12): scripting.go (Manager: script cache,
                     run state, BUSY/KILL, deadlines, RNG), pool.go (M8e R3 VM pool +
                     recycling), bridge.go (redis.* host functions), convert.go
                     (Lua⇄RESP conversion, S7)
web/                 web UI (§10.2): React+TS+vite (bun); embed.go (//go:embed all:dist
                     with committed placeholder) + handler.go (SPA fallback).
                     `make web` builds, `make build` embeds
proto/ultima/v1/     protobuf IDL
gen/                 generated bindings (do not hand-edit; sources: proto/, api/openapi.yaml)
tests/               integration + milestone tests (m3–m8), real sockets, ephemeral ports
tests/ts-roundtrip/  protobuf-es TS round-trip (bun; `bun install` first)
tests/differential/  the parity gate: scripted diffs of replies incl. error strings
                     against a real redis-server
tests/cli-matrix/    CLI command matrix (cases/*.txt) through redis-cli + the three CLIs
third_party/readline/ vendored + patched fork of ergochat/readline v0.1.3 (go.mod
                     `replace`); patch: bare ESC is delivered as its own keypress so
                     vi-mode ESC+<key> works (upstream swallowed both bytes); kept
                     close to upstream — excluded from lint
bin/                 gen.sh, gen-api.sh, gen-build-stamp.sh, bench*.sh, gen-jwt-keys.sh,
                     gen-cli-help.py (regenerates clients/go/ultima/cmdhelp_data.go;
                     needs a Redis 7.2.7 checkout via REDIS_SRC), test-cli-matrix.sh
docs/                ULTIMA-DESIGN.md, implementation-history.md, pluto/ specs, benchmarks/
note/                scratch/reference; gitignored, lint-excluded
```

Adding a new command: implement a handler in the appropriate
`lib/commands/*.go` file, register it in `table.go`'s `init()`, and carry
**Redis-exact semantics and error strings** (see `engine.go` helpers like
`parseIntStrict`, `errUnknownCommand` for the byte-exact conventions).
Then rerun `python3 bin/gen-cli-help.py` (needs `REDIS_SRC`, default
`note/redis-7.2.7`) so the CLI `help`/completion data in
`clients/go/ultima/cmdhelp_data.go` picks it up.

## Build and Test Commands

All via the Makefile (default goal is `build`):

- `make build` — `./ultima-server` with git/build-stamp ldflags
  (`--version` prints it). `make run` runs with `ultima.cfg.json`.
- `make test` — `go test ./...` (unit + integration + differential).
- `make lint` — `golangci-lint run` (v2 config in `.golangci.yml`).
- `make web` / `make test-web` — build the web UI / strict `tsc`
  typecheck. Note: bun *copies* the `file:../clients/typescript` dep —
  after editing `clients/typescript`, rerun `bun install` in `web/`.
- `make build-cli` / `make test-clients` — the three operator CLIs /
  client-library tests and TS+JS builds.
- `make test-cli-matrix` — the CLI command matrix in both security modes;
  `MATRIX_FLAGS=-R` also validates against a real redis-server
  (docs/cli-matrix-testing.md). Requires redis-cli + GNU timeout.
- `make gen_proto` / `make gen_api` — regenerate `gen/` from `proto/` /
  `api/openapi.yaml` (gen_api also syncs the embed copy
  `lib/httpapi/openapi.yaml`).
- `make bench` — `bin/bench.sh`: Ultima vs local redis-server, then
  chains `bench-pubsub.sh` (`BENCH_PUBSUB=0` skips), `bench-m5.sh`
  (`BENCH_M5=0` skips), `bench-m8.sh`, `bench-m9.sh` (`BENCH_M9=0` skips).
  Reports land in `docs/benchmarks/<M>-<date>.md`. bench-m8.sh knobs:
  `BENCH_M8_REQUESTS`, `BENCH_M8_UNTIL=HH:MM` (time-boxed soak),
  `BENCH_M8_POOL_SIZE` (VM-pool A/B), battery gate via `../battery-check`
  (pauses below 50%, resumes at 60%). bench-m9.sh (M9a harness) knobs:
  `BENCH_REPS` (median-of-N, default 3), `BENCH_PAYLOADS`, `BENCH_MEMTIER=0`,
  `BENCH_PROFILE=0`, `BENCH_M9_TAG`; captures pprof snapshots into
  `docs/benchmarks/profiles/` via the `server.pprof_enabled`
  `/debug/pprof/` endpoints.
- `make tidy`, `make clean`.

Configuration: JSON file (`ultima.cfg.json` by default). Defaults come
from struct tags, then the file overrides; `$ENV$NAME` tokens in string
values are expanded from the environment (`lib/config/config.go`).

## Testing Instructions

1. **Unit tests** colocated with code (`lib/**/*_test.go`); goleak guards
   against goroutine leaks in engine tests.
2. **Integration tests** (`tests/`): boot all three surfaces on
   `127.0.0.1:0` and exercise them with real clients; RESP wiring comes
   from the shared `lib/respserver` package (D3).
3. **Differential tests** (`tests/differential/`): scripted command
   sequences run against Ultima (in-process) and a real `redis-server`
   subprocess; decoded replies **including error strings** are diffed —
   this is the primary parity gate. Requires `redis-server` on PATH (or
   `REDIS_BIN`); skipped under `-short` or `DIFFERENTIAL=0`.
   **Standing permission**: `redis-server` and `redis-cli` may be run on
   this machine at any time for probing behavior (there is no data on the
   local Redis that can be broken). Prefer probing the live installed
   server (7.2.7, the compat target) over reading `note/redis/`, which is
   a newer 8.x checkout and can diverge from 7.2.7 behavior. Scripts live
   in `scripts.go` (P0), `scripts_p1.go` (P1), `scripts_m3.go` (P2),
   `scripts_m5.go` (M5), `scripts_m8.go` (M8) — extend the appropriate
   file when adding commands; multi-connection step constructors are
   documented in `compare.go`. Gotcha: config persists across scripts
   within a run — keyspace-notification scripts must reset
   `notify-keyspace-events ""` at the end.

Running `go test ./...` also compiles `note/grpc-vs-text-benchmark` and
`note/crc-probe` (scratch modules kept for reference).

## Code Style Guidelines

- Standard Go; `gofmt`/`goimports` enforced via golangci-lint formatters.
- Enabled linters: errcheck, govet, ineffassign, staticcheck, unused,
  misspell, revive.
- Lint/format **exclusions**: `gen/`, `note/`, `docs/`, `third_party/`, and `lib/resp/`
  (vendored redcon fork — keep it close to upstream; do not restyle it).
- Package doc comments reference design-doc sections (`design doc §N.N`)
  and decision numbers (D1–D20); keep that convention, and update or add
  references when implementing a documented section.
- Comments explain the *why* (Redis-semantics notes, invariants such as
  "call only from inside the shard goroutine"). Byte-exact Redis error
  messages are a hard requirement, validated by the differential harness.
- Never hand-edit files under `gen/`; change `proto/` or
  `api/openapi.yaml` and run `make gen_proto` / `make gen_api`.
- If you change anything this file documents, update this file to match —
  and if the change alters a settled milestone design, also update
  `docs/implementation-history.md`.

## Security Considerations

- `requirepass` auth: when set, the command engine gates every command
  except AUTH/HELLO/QUIT behind `NOAUTH` (`lib/commands/engine.go`).
- `Config.CheckStartupPosture` warns when the RESP listener binds all
  interfaces with no `requirepass` and no TLS.
- Config secrets are injected via `$ENV$NAME` substitution — do not commit
  real credentials to config files.
- Command renaming (Redis's `rename-command`) is deliberately **excluded**
  as security by obscurity (§1.2); protection comes from auth and, later,
  ACLs.
- M6 auth (`lib/auth`, `auth.enabled`): JWT access/refresh tokens signed
  Ed25519 (D20 — key pair read from config file paths, no shared-secret
  mode, no auto-generated keys; `bin/gen-jwt-keys.sh` writes `./keys/`,
  gitignored), TOTP 2FA via `pschlump/htotp`. When enabled, `/api/v1/*`
  (except login/refresh) and the gRPC surface require a Bearer access
  token, the `/ws/v1` upgrade requires `?access_token=` or the
  `bearer, <token>` subprotocol, and the origin policy applies: browser
  upgrades must be same-origin or on `server.ws_origin_allow`
  (comma-separated origins/hosts, `*` = any; no Origin header always
  passes). When auth is disabled (default), the WS endpoint has no origin
  policy (`CheckOrigin: true`) and no upgrade-time auth.
  `note/redis-security-overview.md` is the security reference.
- Command execution must never panic on client input; all errors are reply
  values (`Engine.Execute` contract).
