# CLI Command Matrix Testing

The CLI matrix (`tests/cli-matrix/`, runner `bin/test-cli-matrix.sh`) is an
end-to-end gate that exercises **every Redis 7.2.7 command Ultima
implements** (the `implemented`/`partial` set in
`lib/commands/manifest.json`) against a live `ultima-server`, driving it
through four different clients:

| Target           | Surface | Client binary   | Protocol class |
|------------------|---------|-----------------|----------------|
| `redis-cli`      | RESP    | stock redis-cli | RESP2          |
| `ultima-cli`     | RESP    | M7 operator CLI | RESP2          |
| `ultima-ws-cli`  | WS      | M7 operator CLI | RESP3-class    |
| `ultima-grpc-cli`| gRPC    | M7 operator CLI | RESP3-class    |

Each case is also run in **both security modes** (`noauth` and `auth`), and
with `-R` every expectation is additionally validated against a **real
redis-server 7.2.7** (targets `redis` and `redis3`), so the expected outputs
are proven Redis-correct, not just Ultima-consistent. M8 Lua scripting is
covered by `cases/90-scripting.txt` (all rows probed byte-exact against
7.2.7; dialect-divergent error texts are pinned shape-only via `match`).
Unimplemented Redis commands (functions, streams, geo, …) are pinned to
byte-exact `unknown command` errors in `cases/95-unknown.txt` instead.

## How to run

Prerequisites: the repo builds (`make build build-cli`), `redis-cli` and GNU
`timeout` on PATH (macOS: `brew install coreutils redis`), `openssl` and
`python3` (used for the auth-mode server config). For `-R` you also need
`redis-server`.

```sh
make test-cli-matrix                 # build server + CLIs, run everything
make test-cli-matrix MATRIX_FLAGS=-R # also validate against real redis-server
```

or directly:

```sh
sh bin/test-cli-matrix.sh [options]
  -t TARGET   redis-cli | ultima-cli | ultima-ws-cli | ultima-grpc-cli |
              redis | redis3        (repeatable; default: the 4 ultima targets)
  -m MODE     noauth | auth | both  (default: both)
  -f REGEX    run only cases/specials whose name matches REGEX
  -R          also validate expectations against a real redis-server
  -v          show diffs for failures
```

Useful knobs: `MATRIX_PORT_BASE` (default 16379; RESP=base, gRPC=base+1,
HTTP=base+2, real redis=base+10), `REDIS_CLI`, `REDIS_SERVER`,
`ULTIMA_SERVER`, `ULTIMA_CLI`, `ULTIMA_WS_CLI`, `ULTIMA_GRPC_CLI`,
`CASES_DIR`.

The script starts a fresh `ultima-server` per security mode (temp config,
temp persist dir, empty save rules), FLUSHALLs before every case, and exits
non-zero if anything fails. A final line reports
`N passed, M failed, K skipped`.

## Security modes

- **noauth**: server with no `requirepass`, `auth.enabled` off — pre-M6
  behavior on every surface.
- **auth**: server with `requirepass` *and* `auth.enabled` on (fresh Ed25519
  JWT key pair and a bootstrap admin are generated in the temp dir). The
  per-model differences:
  - `redis-cli` authenticates via `REDISCLI_AUTH` (classic `AUTH`), and the
    runner separately verifies the `NOAUTH`/`WRONGPASS` gates
    (`special:noauth-gate`).
  - `ultima-cli` gets `-a <password>` (its connect handshake AUTHs, so a
    wrong password surfaces as a connect failure).
  - `ultima-ws-cli` / `ultima-grpc-cli` use `--user admin --pass …`: the
    client-side token manager logs in over the management API, fetches a JWT
    access token and presents it at the WS upgrade (`?access_token=`) or as
    gRPC `authorization: bearer` metadata. `special:bad-token` verifies a
    garbage JWT is rejected.

## Case file format

Cases live in `tests/cli-matrix/cases/*.txt` (read in filename order):

```
# comments are allowed anywhere; they are ignored by the parser
== case: <group>.<name>          # unique; convention: file-group prefix
== type: exact                   # exact | sorted | match | smatch (def. exact)
== targets: all                  # comma-list: resp2, resp3, no-redis
== modes: all                    # all | noauth-only | auth-only
SET alpha one
GET alpha
--
OK
"one"
== resp3                         # optional override for RESP3-class targets
--
OK
"one"
```

- All input lines of a case go down **one connection** (the clients' stdin
  pipe mode), so MULTI/EXEC, SELECT and WATCH behave sessionfully. The
  server is FLUSHALLed before each case.
- Expected output is the redis-cli formatted style (`OK`, `(integer) 3`,
  `"blob"`, `(nil)`, numbered arrays — right-aligned indices for 10+
  elements, `1# k => v` maps, `(double)`/`` `(empty array|set|hash)` ``).
  All four clients render identically by design (the ultima CLIs share
  `clients/go/ultima/render.go`, and the runner invokes redis-cli with
  `--no-raw` to force the same formatted style).
- Comparison types: `exact` byte-diff; `sorted` line-sort then diff;
  `match` per-line extended regexes; `smatch` sorted regex match. Use
  `match`/`smatch` for nondeterministic values (TTLs, CLIENT ID); beware
  that rendered array indices pin line positions, so unordered replies
  usually want `match` with alternation regexes rather than `sorted`.
- `== resp3` supplies the expectation for RESP3-class targets when the wire
  types differ (doubles for ZSCORE & co., maps for HGETALL/CONFIG GET,
  nested pairs for HRANDFIELD … WITHVALUES). `== resp3: skip` skips the case
  there instead. With `-R`, the `redis` and `redis3` targets validate both
  blocks against real Redis.
- Never put SUBSCRIBE/PSUBSCRIBE/UNSUBSCRIBE/PUNSUBSCRIBE/MONITOR in a case
  (the CLIs stream until SIGINT), and never issue a blocking command
  (BLPOP & co.) against an empty key. Both are covered by "specials" in the
  runner instead: `special:pubsub-delivery`, `special:psubscribe-delivery`,
  `special:blpop-wakeup`, `special:monitor-stream`, `special:quit`,
  `special:noauth-gate`, `special:bad-token`. Each case has a 30 s timeout
  guard so a mistake fails the case instead of hanging the run.

## Adding a case for a new command

1. Add a case block to the appropriate `cases/*.txt` (keep the group-prefix
   naming).
2. If unsure of a reply or error string, probe the real thing:
   `printf 'CMD ...\n' | redis-cli --no-raw -p <port>` (and with `-3` for
   the RESP3 block) against a throwaway `redis-server --port <port>
   --save '' --appendonly no`.
3. Run `MATRIX_PORT_BASE=17400 sh bin/test-cli-matrix.sh -f '^<group>\.' -R`
   until it reports `0 failed`.

## Known-good divergences pinned by the suite

Ultima-only expectations (`== targets: no-redis`): `COMMAND COUNT` (146 vs
Redis's ~240), `COMMAND INFO` entry shape (6 fields vs Redis's 10), and the
`unknown.*` file (unimplemented commands error in Ultima, work in Redis).

Bugs this harness caught on first contact: `SET … GET` on a wrong-type key
silently overwrote it; EXPIRE rejected the legal `XX GT`/`XX LT` option
pairs; EXEC's own arity error came back as a plain arity error instead of
the EXECABORT form; the CLIs rendered RESP3 sets/maps as arrays/`(empty
map)` instead of `1~ …`/`(empty hash)`; and QUIT over WebSocket could drop
its `OK` reply (the session then reported it ABORTED on resume). All fixed;
regression cases live in the group files.
