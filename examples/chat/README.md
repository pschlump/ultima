# Chat example (design doc §11.4 #2)

A live chat room: browser clients plus a Go bot (`examples/chat/bot`) that
joins the room, posts a welcome message, and answers `!ping` / `!time`.

**What it demonstrates**

- Pub/sub topics as channels: messages are JSON `{"user","text","ts"}`
  published on `chat:lobby`.
- List history: every post is also `RPUSH`ed to `chat:lobby:hist` and the
  list is capped with `LTRIM chat:lobby:hist -100 -1`; joining clients load
  the backlog with `LRANGE chat:lobby:hist 0 -1`. (The page and the bot use
  the same post sequence; the bot's lives in `examples/chat/botcore`.)
- Keyspace-notification presence: each client maintains
  `chat:presence:<user>` with a 15s PX refreshed on a 10s timer and publishes
  a heartbeat on `chat:presence`; the online list adds users from those beats
  and removes them when the `__keyevent@0__:expired` notification for their
  key arrives. The bot enables this on startup with
  `CONFIG SET notify-keyspace-events Ex` (keyevent class + expired events).
- Reconnect without loss (§9.4): the page and the bot run on resumable WS
  sessions — subscriptions and in-flight pushes survive a network drop; a
  status pill and note line show resume vs. fresh reconnect.
- Both halves use the M7 client libraries: the bot uses the Go client
  (`clients/go/ultima`), the page the vendored ESM bundle of the JS client
  (`clients/javascript`, copied to `static/vendor/ultima-client.js` — refresh
  it with `cp ../../clients/javascript/dist/esm/index.js` after a client
  rebuild).

## Run

```
make build && ./ultima-server     # the server (any config; auth off)
go run ./examples/chat/bot        # joins chat:lobby + serves the page
```

Open http://127.0.0.1:8091 (open it twice for two users; `?user=name` picks
a name, otherwise one is generated and kept in localStorage). Flags: `-addr`
(Ultima HTTP/WS host:port, default 127.0.0.1:6381), `-http` (page listener,
default :8091). The page's server address defaults to 127.0.0.1:6381 and is
overridable with `?server=host:port`.

## Auth

Like the leaderboard example, the page has no login flow: run the demo
against a server with `auth.enabled=false` (the default). The page is served
from a different origin than the server; that is fine while auth is off (the
M6d origin policy only applies when auth is enabled).

## Verification

`tests/m7_examples_test.go` runs the whole flow against an in-process server
with `notify-keyspace-events` on: two WS clients (subscribe/publish), the
bot's real push-driven reply path (`botcore.Handle`), history contents and
the 100-entry cap, and a presence key expiring into the
`__keyevent@0__:expired` push. The page was smoke-tested by serving it,
curling the assets, and replaying its exact client calls from a bun script.
