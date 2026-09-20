# @ultima/client

TypeScript client for **Ultima** (design doc §11.2): the `/ws/v1` WebSocket
command surface with §9.4 resumable sessions, the `/api/v1` REST management
API, and M6a token management. Framework-agnostic — runs on bun, Node (≥18;
≥22 for the global `WebSocket`), and in the browser. The React hooks are a
separate subpath (`./react`); the core never imports react.

The protobuf bindings are imported repo-relative from `gen/ts` (like `web/`
and `tests/ts-roundtrip` do); publishable npm packaging is a later concern.

## Layout

| File            | Contents                                                        |
|-----------------|------------------------------------------------------------------|
| `src/ws.ts`     | `UltimaWS` — typed commands, generic `exec`, pub/sub, §9.4 recovery |
| `src/rest.ts`   | `RestClient` — typed `/api/v1/*` wrappers                        |
| `src/auth.ts`   | `AuthManager` — login/refresh/re-login, auth-disabled probe      |
| `src/client.ts` | `UltimaClient` — umbrella tying auth + REST + WS together        |
| `src/react.ts`  | `useUltima` / `useSubscription` hooks (optional subpath)         |
| `src/value.ts`  | reply-`Value` unwrappers; `ReplyError`                           |

## Quick start

```ts
import { UltimaClient } from "@ultima/client";

const client = new UltimaClient({
  baseUrl: "http://127.0.0.1:6381",              // or { host, httpPort, secure }
  credentials: { username: "admin", password: "…" }, // only when auth.enabled
});
client.connect();

await client.ws.set("k", "v", { px: 60_000 });   // typed helpers
await client.ws.get("k");                        // → "v"
await client.ws.exec("echo", "hi");              // generic escape hatch (raw Value)

await client.subscribe("events", (msg) => console.log(msg.channel, msg.text));
```

`UltimaWS` keeps a resumable session (§9.4): on a network drop it reconnects
with backoff, resumes the session, and the server replays missed pushes.
Subscriptions are tracked client-side and re-issued automatically when a
session is unrecoverable. Events: `onResumed` (resume succeeded),
`onGap(sessionId)` (SESSION_EXPIRED — pushes were lost), `onStateChange`.
Error replies: `exec()` resolves with the error `Value` (RESP-style); the
typed helpers throw `ReplyError`.

REST: `client.rest.info()`, `scanKeys(cursor, match, count, db)` (cursor is a
string, `"0"` = done), `getKey`/`deleteKey`, `getConfig`/`putConfig`, the
`users*` admin endpoints, `save`/`bgsave`/`bgrewriteaof`. Auth:
`client.auth.probe()` tells whether the server requires auth;
`client.login(user, pass, totp?)` logs in; tokens refresh proactively before
the JWT `exp` and re-login with the stored credentials when the refresh
token dies.

## React

```tsx
import { useUltima, useSubscription } from "@ultima/client/react";

function Leaderboard() {
  const { client, state } = useUltima({ baseUrl: "http://127.0.0.1:6381" });
  useSubscription(client, "scores", (msg) => { /* … */ });
  // …
}
```

## Development

- `bun install`
- `bun run typecheck` — strict `tsc --noEmit` (incl. `noUncheckedIndexedAccess`)
- `bun test` — the round-trip; requires a live server:
  `WS_URL=ws://127.0.0.1:6381/ws/v1 HTTP_URL=http://127.0.0.1:6381 bun test/roundtrip.ts`
  (add `AUTH=1 USER=… PASS=…` against an auth-enabled server).
  `go test ./tests/ -run M7TS` drives both variants in-process.
