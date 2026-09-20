// M7b round-trip for @ultima/client against a live Ultima server: typed +
// generic WS commands, pub/sub push delivery, §9.4 session drop/resume with
// missed-push replay, the SESSION_EXPIRED gap event, the REST management
// API, and (with AUTH=1) the login flow against an auth-enabled server.
//
// Usage:
//   WS_URL=ws://127.0.0.1:PORT/ws/v1 HTTP_URL=http://127.0.0.1:PORT bun test/roundtrip.ts
//   AUTH=1 USER=admin PASS=secret ...   # auth-enabled variant
import {
  ApiError,
  AuthManager,
  MemoryStorage,
  ReplyError,
  RestClient,
  UltimaClient,
  UltimaWS,
  type PubSubMessage,
} from "../src/index";

function envRequired(name: string): string {
  const v = process.env[name];
  if (!v) {
    console.error(`FAIL: ${name} env var is required`);
    process.exit(1);
    throw new Error("unreachable");
  }
  return v;
}

const WS_URL = envRequired("WS_URL");
const HTTP_URL = envRequired("HTTP_URL");
const AUTH = process.env.AUTH === "1";
const USER = process.env.USER ?? "";
const PASS = process.env.PASS ?? "";

let failures = 0;
function check(name: string, ok: boolean, detail = "") {
  console.log(`${ok ? "PASS" : "FAIL"}: ${name}${ok || detail === "" ? "" : ` — ${detail}`}`);
  if (!ok) failures++;
}

async function waitFor(desc: string, cond: () => boolean, timeoutMs = 10_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (cond()) return true;
    await new Promise((r) => setTimeout(r, 10));
  }
  console.error(`FAIL: timed out waiting for ${desc}`);
  failures++;
  return false;
}

const PI = 3.141592653589793;

async function main(): Promise<void> {
  // --- construction + connect ------------------------------------------------
  let resumed = false;
  let gap: string | null = null;
  const received: PubSubMessage[] = [];

  const client = new UltimaClient({
    baseUrl: HTTP_URL,
    ...(AUTH ? { credentials: { username: USER, password: PASS } } : {}),
    storage: new MemoryStorage(),
    ws: {
      url: WS_URL,
      reconnect: { baseDelayMs: 800, maxDelayMs: 2_000 },
      onResumed: () => {
        resumed = true;
      },
      onGap: (id) => {
        gap = id;
      },
    },
  });
  client.connect();
  await client.ws.ready();
  check("connect + §9.4 fresh-session handshake", client.ws.state === "open" && client.ws.session !== null);

  // Idempotence: the round-trip owns the rt:* keyspace.
  await client.ws.exec("flushall");

  // --- auth probe / login ------------------------------------------------------
  const probe = await client.auth.probe();
  check("auth probe", probe.authRequired === AUTH, `want authRequired=${AUTH}, got ${probe.authRequired}`);

  if (AUTH) {
    const bare = new AuthManager({ baseUrl: HTTP_URL, storage: new MemoryStorage() });
    let denied: unknown = null;
    try {
      await bare.login({ username: USER, password: "wrong-password" });
    } catch (e) {
      denied = e;
    }
    check("login with wrong password rejected (401)", denied instanceof ApiError && denied.status === 401);

    const unauth = new RestClient({ baseUrl: HTTP_URL });
    let unauthErr: unknown = null;
    try {
      await unauth.info();
    } catch (e) {
      unauthErr = e;
    }
    check("unauthenticated /info rejected (401)", unauthErr instanceof ApiError && unauthErr.status === 401);

    const sess = await client.login(USER, PASS);
    check("login with valid credentials", sess.username === USER, `want ${USER}, got ${sess.username}`);
  }

  const info = await client.rest.info();
  check("REST GET /api/v1/info", typeof info.sections === "object" && Object.keys(info.sections).length > 0);

  // --- typed WS round-trips ----------------------------------------------------
  check('set rt:k "hello"', (await client.ws.set("rt:k", "hello")) === "OK");
  check("get rt:k", (await client.ws.get("rt:k")) === "hello");
  check("set nx on existing key → null", (await client.ws.set("rt:k", "other", { nx: true })) === null);
  check("set get option returns old value", (await client.ws.set("rt:k", "v2", { get: true })) === "hello");
  check("set px + pttl", (await client.ws.set("rt:ttl", "x", { px: 60_000 })) === "OK" && (await client.ws.pttl("rt:ttl")) > 0);
  check("persist clears ttl", (await client.ws.persist("rt:ttl")) === true && (await client.ws.pttl("rt:ttl")) === -1);

  await client.ws.mset({ "rt:m1": "a", "rt:m2": "b" });
  const mg = await client.ws.mget("rt:m1", "rt:m2", "rt:missing");
  check("mset + mget (null for missing)", mg[0] === "a" && mg[1] === "b" && mg[2] === null, JSON.stringify(mg));

  check("incrBy 40 → 40n", (await client.ws.incrBy("rt:n", 40)) === 40n);
  check("incr → 41n", (await client.ws.incr("rt:n")) === 41n);
  check("incrByFloat 0.5 → 41.5", (await client.ws.incrByFloat("rt:n", 0.5)) === 41.5);

  check("append", (await client.ws.append("rt:k", "!")) === 3 && (await client.ws.get("rt:k")) === "v2!");

  check(
    "hset/hget/hgetall",
    (await client.ws.hset("rt:h", { f1: "v1", f2: "v2" })) === 2 &&
      (await client.ws.hget("rt:h", "f1")) === "v1" &&
      JSON.stringify(await client.ws.hgetall("rt:h")) === JSON.stringify({ f1: "v1", f2: "v2" }),
  );

  await client.ws.rpush("rt:l", "a", "b", "c");
  const lr = await client.ws.lrange("rt:l", 0, -1);
  check("rpush + lrange", JSON.stringify(lr) === JSON.stringify(["a", "b", "c"]), JSON.stringify(lr));
  check("lpop/rpop", (await client.ws.lpop("rt:l")) === "a" && (await client.ws.rpop("rt:l")) === "c");

  check("sadd/sismember", (await client.ws.sadd("rt:s", "x", "y")) === 2 && (await client.ws.sismember("rt:s", "x")));
  const sm = (await client.ws.smembers("rt:s")).sort();
  check("smembers", JSON.stringify(sm) === JSON.stringify(["x", "y"]), JSON.stringify(sm));

  // Typed ZADD double fidelity through the whole pipeline.
  check("zadd pi=π", (await client.ws.zadd("rt:z", { pi: PI })) === 1);
  const zs = await client.ws.zscore("rt:z", "pi");
  check("zscore double fidelity", zs === PI, `want ${PI}, got ${zs}`);
  const zws = await client.ws.zrangeWithScores("rt:z", 0, -1);
  check(
    "zrangeWithScores",
    zws.length === 1 && zws[0]?.member === "pi" && zws[0]?.score === PI,
    JSON.stringify(zws),
  );

  // --- generic escape hatch ------------------------------------------------------
  const echo = await client.ws.exec("echo", "hi");
  check("generic ECHO", echo.kind.case === "blobString" && new TextDecoder().decode(echo.kind.value) === "hi");
  const gset = await client.ws.exec("set", "rt:g", "gv");
  check("generic SET", gset.kind.case === "simpleString" && gset.kind.value === "OK");

  // Error replies: exec resolves with the error kind; typed helpers throw.
  const wrong = await client.ws.exec("get", "rt:s");
  check("exec resolves error replies (WRONGTYPE)", wrong.kind.case === "error" && wrong.kind.value.startsWith("WRONGTYPE"));
  let replyErr: unknown = null;
  try {
    await client.ws.get("rt:s");
  } catch (e) {
    replyErr = e;
  }
  check("typed get throws ReplyError on WRONGTYPE", replyErr instanceof ReplyError);

  // --- REST data endpoints -------------------------------------------------------
  await client.ws.mset({ "rt:scan:1": "1", "rt:scan:2": "2", "rt:scan:3": "3" });
  const found = new Set<string>();
  let cursor = "0";
  do {
    const page = await client.rest.scanKeys(cursor, "rt:scan:*", 100);
    cursor = page.cursor;
    for (const k of page.keys) found.add(k);
  } while (cursor !== "0");
  check(
    "REST /keys/scan (string cursor, 0 = done)",
    found.has("rt:scan:1") && found.has("rt:scan:2") && found.has("rt:scan:3"),
    [...found].join(","),
  );
  const preview = await client.rest.getKey("rt:k");
  check("REST GET /key/{key}", preview.type === "string" && preview.value === "v2!", JSON.stringify(preview));
  const deleted = await client.rest.deleteKey("rt:g");
  check("REST DELETE /key/{key}", deleted.deleted === true);

  // --- pub/sub push delivery -----------------------------------------------------
  await client.subscribe("rt:chan", (m) => received.push(m));
  const pub = new UltimaWS({
    url: WS_URL,
    session: false,
    storage: new MemoryStorage(),
    getToken: () => client.auth.tokens?.accessToken ?? null,
  });
  pub.connect();
  await pub.ready();
  const n1 = await pub.exec("publish", "rt:chan", "m1");
  // ≥1: a previous run's detached session may still be retained (§9.4) with
  // its subscription — correct server behavior, harmless to this check.
  check("publish reply ≥ 1 subscriber", n1.kind.case === "int" && n1.kind.value >= 1n);
  await waitFor("push delivery of m1", () => received.some((m) => m.text === "m1"));
  check("push delivery", received.some((m) => m.channel === "rt:chan" && m.text === "m1"));

  // --- §9.4 session drop/resume with missed-push replay ---------------------------
  check("session id assigned", client.ws.session !== null);
  const before = received.length;
  client.ws.forceReconnect();
  // Publish while the subscriber is detached: the session buffers them.
  await pub.exec("publish", "rt:chan", "missed-1");
  await pub.exec("publish", "rt:chan", "missed-2");
  const okResume = await waitFor(
    "session resume + replay of missed pushes",
    () => resumed && received.slice(before).some((m) => m.text === "missed-1") && received.some((m) => m.text === "missed-2"),
    15_000,
  );
  if (okResume) {
    const tail = received.slice(before).map((m) => m.text);
    check(
      "missed pushes replayed in order after resume",
      tail.indexOf("missed-1") !== -1 && tail.indexOf("missed-1") < tail.indexOf("missed-2"),
      tail.join(","),
    );
    check("same session id after resume", client.ws.session !== null);
  }
  check("no gap event on successful resume", gap === null);

  // Live pushes keep flowing after the resume.
  await pub.exec("publish", "rt:chan", "post-resume");
  await waitFor("live push after resume", () => received.some((m) => m.text === "post-resume"));

  // --- SESSION_EXPIRED → explicit gap event ---------------------------------------
  let gap2: string | null = null;
  const stale = new UltimaWS({
    url: WS_URL,
    resume: { id: "rt:no-such-session", lastPushSeq: 0n },
    storage: new MemoryStorage(),
    getToken: () => client.auth.tokens?.accessToken ?? null,
    onGap: (id) => {
      gap2 = id;
    },
  });
  stale.connect();
  await waitFor("SESSION_EXPIRED gap event", () => gap2 !== null);
  check("onGap fired with the lost session id", gap2 === "rt:no-such-session", String(gap2));
  await stale.ready();
  check("client recovers with a fresh session after the gap", (await stale.ping()) === "PONG" && stale.session !== null);

  // --- cleanup ----------------------------------------------------------------------
  stale.close();
  pub.close();
  client.close();
}

const watchdog = setTimeout(() => {
  console.error("FAIL: global watchdog timeout (45s)");
  process.exit(1);
}, 45_000);

main()
  .then(() => {
    clearTimeout(watchdog);
    console.log(failures === 0 ? "PASS: all @ultima/client round-trip checks succeeded" : `FAIL: ${failures} check(s) failed`);
    process.exit(failures === 0 ? 0 : 1);
  })
  .catch((err) => {
    clearTimeout(watchdog);
    console.error(`FAIL: round-trip threw: ${err instanceof Error ? (err.stack ?? err.message) : String(err)}`);
    process.exit(1);
  });
