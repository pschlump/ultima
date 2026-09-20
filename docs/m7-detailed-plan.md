# M7 detailed plan — Client libraries + example applications

Design doc §14.4 M7: "Client libraries (§11) + example applications".
Exit criteria: **Go + TS + JS packages build; CLIs run on the Go client;
leaderboard + chat examples run end-to-end.**

Execution is phased M7a → M7d, each phase independently green
(`go test ./...`, `make lint`, `make web`, `make test-web`), matching the
M5/M6 convention.

**Status: M7 complete — M7a (Go client + CLIs), M7b (TS client + JS
dist + web refactor onto @ultima/client), M7c (leaderboard + chat
examples, e2e green) and M7d (wiring/docs) all done. Memo:
`note/M7-implemented.md`.**

---

## Context

- §11.1: Go lib `clients/go` — gRPC + WS + REST behind one client type;
  the operator CLIs (§6.4: `ultima-cli` RESP, `ultima-ws-cli`,
  `ultima-grpc-cli`) are thin shells over it.
- §11.2: TS lib `clients/typescript` (`@ultima/client`) — REST + WS,
  token management, §9.4 recovery, framework-agnostic core + React
  hooks; the web UI is its first consumer.
- §11.3: `clients/javascript` — compiled ESM + CJS builds of the TS
  library with bundled `.d.ts`.
- §11.4: five examples; the M7 exit criterion names only leaderboard +
  chat — ticker/presence/log-tail are deferred (log-tail needs M8
  streams anyway).
- Reusable pieces: generated gRPC client in `gen/go/ultima/v1`
  (`UltimaClient`: Exec bidi, ExecBatch, ExecGeneric, Subscribe,
  Monitor, Ping); `lib/envelope` resp.Value↔proto conversions; the web
  UI's `web/src/lib/ws.ts` + `api.ts` already implement ~90% of the TS
  client; RESP reply parsing exists only in test code
  (`tests/differential/harness.go`), so clients/go gets its own small
  RESP2/3 parser producing `resp.Value`.

## M7a — Go client (`clients/go/ultima`) + CLIs

- Same Go module; package `ultima`. Files: `resp.go` (RESP client: TCP,
  optional AUTH/SELECT on connect, RESP2/3 reply parser incl. push
  frames, `Exec`, `Subscribe`), `grpc.go` (bidi Exec stream with seq
  correlation + pending map; typed helpers for the ~30 typed envelope
  commands; ExecGeneric/ExecBatch; Subscribe/Monitor streams; Ping),
  `ws.go` (`/ws/v1`, same typed helpers, full §9.4 recovery: session
  handshake, push_seq replay, backoff reconnect, transparent
  re-subscription, ABORTED fails the pending call, SESSION_EXPIRED → Gap
  event), `rest.go` (`/api/v1/*`: auth, info, shards, keys/scan,
  key get/delete, config, admin users), `auth.go` (shared token manager:
  login with TOTP, proactive refresh, re-login on refresh failure,
  auth-disabled probe), `client.go` (umbrella Client + Options),
  `render.go` (redis-cli-style reply rendering shared by the CLIs).
- CLIs: `cmd/ultima-cli` (RESP, redis-cli analogue: `-h -p -a -n`),
  `cmd/ultima-ws-cli` (`-addr`, `--user/--pass/--totp` or `--token`),
  `cmd/ultima-grpc-cli` (`-addr`). One-shot + REPL modes; subscribe
  commands stream pushes until Ctrl-C; error replies → stderr, exit 1
  in one-shot.
- Tests: `clients/go/ultima/*_test.go` boot all three surfaces on
  ephemeral ports (tests/integration_test.go wiring pattern);
  round-trips, typed helpers, ExecBatch, Subscribe/Monitor, WS
  drop/resume replay, auth-enabled variant. `tests/m7_cli_test.go` runs
  the built CLI binaries (one-shot + piped REPL).

## M7b — TypeScript client (`clients/typescript`) + JS dist (`clients/javascript`)

- `@ultima/client`: `src/ws.ts` (generalized from web/src/lib/ws.ts:
  typed helpers, generic exec, `subscribe(channel, handler)`, §9.4
  recovery with explicit `gap` event on SESSION_EXPIRED), `src/rest.ts`,
  `src/auth.ts` (token manager), `src/client.ts` (umbrella),
  `src/index.ts`, `src/react.ts` (`useUltima`/`useSubscription`).
  Framework-agnostic core (bun/Node/browser); protobuf-es bindings
  imported repo-relative from `gen/ts` (publishable packaging is a later
  concern). Strict tsconfig; `@bufbuild/protobuf` version matches
  tests/ts-roundtrip.
- Tests: `clients/typescript/test/roundtrip.ts` (bun, env-driven like
  tests/ts-roundtrip): typed + generic round-trip, subscribe push,
  session drop/resume replay, SESSION_EXPIRED gap, REST login,
  ZADD double fidelity. Driven by `tests/m7_ts_test.go` (mirrors
  `tests/m4_ts_test.go`, graceful skip without bun).
- `clients/javascript`: ESM + CJS bundles of @ultima/client
  (`bun build --format=esm|cjs`) + `.d.ts` (`tsc
  --emitDeclarationOnly`); own package.json `exports`; smoke pages
  proving bun/node/`<script type="module">` loading.
- Web UI refactor onto `@ultima/client` (bun `file:` dep + vite alias) —
  the §11.2 "first consumer" made real; lands as its own step after the
  library is green, with `make web` + live smoke re-verified.

## M7c — Examples (`examples/`, §11.4 — the two exit examples)

- `examples/leaderboard/`: browser page (JS-client ESM build, no build
  step), live ZADD leaderboard, updates over pub/sub on the recovered WS
  session; Go companion `submitter` (on clients/go) simulates scores
  (`--rate`, `--players`) and serves the page (embed.FS).
- `examples/chat/`: browser chat; channels as pub/sub topics, history in
  capped LISTs, presence via expiring keys + keyspace notifications
  (`__keyevent@0__:expired`); Go companion `bot` joins/echoes and serves
  the page.
- Each example has a README (the §11.4 documentation chapter).
- E2E: `tests/m7_examples_test.go` — leaderboard submitter core + Go
  client verification; chat flow with history + presence expiry
  (notify-keyspace-events on).

## M7d — Wiring & docs

- Makefile: `build-cli` (three CLIs to repo root, `clean` removes them),
  `test-clients` (go test ./clients/... + TS roundtrip). `.gitignore`
  the CLI binaries.
- Update: this document's status, `docs/ULTIMA-DESIGN.md` M7 row,
  `AGENTS.md` (layout entries for clients/, examples/, cmd/ CLIs; build
  commands; M7 status), `note/M7-implemented.md` memo.

## Validation (M7 done gate)

1. `make build`, `make build-cli`, `go test ./...`, `make lint` green.
2. `make web` + `make test-web` green after the @ultima/client refactor.
3. CLI smoke: all three CLIs one-shot + REPL against a live server
   (auth-disabled and auth-enabled).
4. TS strict tsc + bun roundtrip green (incl. session resume);
   `clients/javascript` ESM/CJS load under bun and node.
5. Examples e2e: `go test -run M7 ./tests/` green; leaderboard demo
   manually smoke-verified in a browser.

## Out of scope for M7

- §11.4 ticker / presence-board / log-tail examples (log-tail needs M8
  streams).
- RESP TLS in the client; publishing @ultima/client to npm; cluster
  support.
