# Leaderboard example (design doc §11.4 #1)

A real-time game leaderboard: a Go score submitter writes into a sorted set
and publishes every update; a browser page renders the top 10 live.

**What it demonstrates**

- Sorted sets: scores are `ZADD lb:scores <score> <player>`; the page reads
  the board with `ZRANGE lb:scores -10 -1 WITHSCORES`.
- Pub/sub: each submission also `PUBLISH`es a JSON `{"player","score"}`
  payload on `lb:update`; the page subscribes and refreshes on each push.
- Reconnect without loss (§9.4): the page runs on a resumable WS session.
  Kill the network (or click **simulate network drop**, which calls the
  client's `forceReconnect`) while scores flow: on reconnect the session
  resumes, buffered pushes replay in order, and the "session resumed" note
  appears. If the server's retention window lapses, the "session expired"
  note appears instead and the board is re-read from scratch.
- Both halves use the M7 client libraries: the submitter uses the Go client
  (`clients/go/ultima`, WS surface), the page the vendored ESM bundle of the
  JS client (`clients/javascript`, copied to `static/vendor/ultima-client.js` —
  refresh it with `cp ../../clients/javascript/dist/esm/index.js` after a
  client rebuild).

## Run

```
make build && ./ultima-server            # the server (any config; auth off)
go run ./examples/leaderboard/submitter  # submits scores + serves the page
```

Open http://127.0.0.1:8090. Flags: `-addr` (Ultima HTTP/WS host:port,
default 127.0.0.1:6381), `-rate` (scores/sec, default 5), `-players`
(default 20), `-http` (page listener, default :8090). The page's server
address field (or `?server=host:port`) must point at the Ultima HTTP/WS
address.

## Auth

With `auth.enabled=true` the demo has no login flow: the WS upgrade requires
a Bearer token, so the page is meant for auth-disabled servers. (The
submitter would need `Options.Token`/`Username` wired through flags — not
done here to keep the example small.) The page is served from a different
origin than the server; that is fine while auth is off (the M6d origin
policy only applies when auth is enabled).

## Verification

`tests/m7_examples_test.go` drives the submitter core
(`examples/leaderboard/submit`) against an in-process server and asserts the
board contents and the in-order `lb:update` pushes. The page was smoke-tested
by serving it, curling the assets, and replaying its exact client calls
(subscribe `lb:update`, `zrangeWithScores`) from a bun script.
