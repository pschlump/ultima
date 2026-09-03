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

**Current status**: M0 (skeleton, three listeners) and M1 (shard engine,
RESP front-end, P0 commands) are implemented and committed. Later milestones
from the design layout (§14.1: `lib/types`, `lib/persist`, `clients/`,
`web/`, `api/`, extra CLIs under `cmd/`) do **not** exist yet.

## Technology Stack

- **Go 1.27** (module `github.com/pschlump/ultima`).
- Key dependencies (see `go.mod`):
  - `github.com/pschlump/pluto` — generic data-structure library (thread-safe
    `_ts` variants). **Pulled in via `replace` directive to the sibling
    checkout `../pluto`** — that directory must exist for the module to
    build. Its per-structure specs live in `docs/pluto/`.
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
| HTTP/WebSocket | `:6381`      | `lib/handler` mounted on a chi mux (`cmd/ultima-server/router.go`) |

Request flow: each front-end parses its wire format, calls
`commands.Engine.Execute(ConnState, args)`, and renders the returned
`resp.Value` (decision D3: one command engine, three front-ends).

- **Sharding** (`lib/shard`): the keyspace of each logical DB lives in one
  pluto `sharded_hash_ts.ShardedHash` whose stripe count equals the shard
  count; key → shard routing is `crc64(key) & (N-1)`. Each shard has an
  owner goroutine draining a task queue (`Engine.Do` / `DoMulti` /
  `DoShard`); all data-mutating work runs inside the owning shard's
  goroutine — no caller-side locks on the hot path. Shard-count helpers
  only run inside the shard goroutine (see the comment block at
  `lib/shard/shard.go:380`).
- **Expiry**: passive on access plus an exact per-shard min-heap
  (`heap_ts`) swept periodically by the shard goroutine (D5).
- Shard count: config `shard_count`, `0` = 4×GOMAXPROCS, rounded up to a
  power of two (`shard.ResolveShardCount`).
- Graceful shutdown: SIGINT/SIGTERM → drain gRPC, close HTTP and RESP,
  stop shard goroutines (10 s cap).

**Commands implemented so far (M1/P0)**: connection (PING, ECHO, HELLO,
AUTH, SELECT, QUIT), strings (SET/GET family, INCR/DECR family, APPEND,
STRLEN, MGET/MSET/MSETNX), keyspace (DEL, EXISTS, EXPIRE/PEXPIRE, TTL/PTTL,
PERSIST, TYPE, SCAN), server (INFO, DBSIZE, FLUSHDB/FLUSHALL, CONFIG,
CLIENT, COMMAND). Only the **string** value type exists; hashes/lists/sets/
zsets/streams arrive in M2+. Ultima reports Redis compatibility version
7.2.7 (`commands.CompatVersion`).

## Code Organization

```
cmd/ultima-server/   main binary: main.go, startup.go (wiring), router.go (chi), version.go (build-stamp vars)
lib/config/          JSON config: `default:"..."` struct tags via reflection + `$ENV$NAME` env substitution (D6)
lib/resp/            vendored + extended fork of tidwall/redcon v1.6.4 (D2); adds RESP3 emitters,
                     per-connection protocol versioning; kept close to upstream — excluded from lint
lib/shard/           sharded keyspace engine, owner goroutines, routing, expiry heap
lib/commands/        front-end-agnostic command engine; table.go is the command registry
                     (def(name, arity, flags, first, last, step, group, handler))
lib/grpcsrv/         gRPC front-end (M0: Ping only)
lib/handler/         HTTP/WS routes (/health, /ready, /api/v1/ping, /ws/v1 stub)
proto/ultima/v1/     protobuf IDL
gen/go/ultima/v1/    generated protobuf Go bindings (do not hand-edit)
tests/               integration_test.go (boots all three surfaces on ephemeral ports)
tests/differential/  harness diffs replies against a real redis-server (the parity gate)
bin/                 gen.sh (protoc), gen-build-stamp.sh (ldflags), bench.sh (benchmark sweep)
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
- `make bench` — `bin/bench.sh`: Ultima vs local `redis-server` via
  `redis-benchmark`; writes a report to `docs/benchmarks/M1-<date>.md`.
  Tunable via `REDIS_BIN`, `BENCH_BIN`, `BENCH_*_PORT`, `BENCH_REQUESTS`.
- `make tidy`, `make clean`.

Configuration: JSON file (`ultima.cfg.json` by default). Defaults come
from struct tags, then the file overrides; `$ENV$NAME` tokens in string
values are expanded from the environment (`lib/config/config.go`).

## Testing Instructions

1. **Unit tests** colocated with code (`lib/**/*_test.go`); goleak guards
   against goroutine leaks in engine tests.
2. **Integration tests** (`tests/integration_test.go`): boot all three
   listener surfaces on `127.0.0.1:0` and exercise them with real clients.
   `cmd/ultima-server` is not importable, so the test re-wires the same
   `lib` packages as `startup.go`/`router.go` — keep them in sync when
   changing wiring.
3. **Differential tests** (`tests/differential/`): scripted command
   sequences run against Ultima (in-process, ephemeral port) and a real
   `redis-server` subprocess; decoded replies **including error strings**
   are diffed — this is the primary parity gate. Requires `redis-server`
   on PATH (or `REDIS_BIN`); skipped under `-short` or `DIFFERENTIAL=0`.
   Extend `scripts.go` when adding commands.

Running `go test ./...` also compiles `note/grpc-vs-text-benchmark`
(a scratch benchmark module kept for reference).

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
- The M0 WebSocket stub has no origin policy (`CheckOrigin: true`); real
  auth (JWT + TOTP 2FA via `pschlump/htotp`) is scheduled for M6 (§9).
  `note/redis-security-overview.md` is the security reference.
- Command execution must never panic on client input; all errors are reply
  values (`Engine.Execute` contract).
