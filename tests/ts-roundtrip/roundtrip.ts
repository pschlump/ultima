// M4 exit criterion: TypeScript round-trip against the Ultima WebSocket
// front-end (/ws/v1, design doc §6.3, decision D15). Sends one binary
// protobuf Command frame per command and asserts each CommandResponse.
//
// Usage: WS_URL=ws://127.0.0.1:PORT/ws/v1 bun roundtrip.ts
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  CommandSchema,
  CommandResponseSchema,
  type Command,
  type Value,
} from "../../gen/ts/ultima/v1/command_pb";

const WS_URL = process.env.WS_URL;
if (!WS_URL) {
  console.error("FAIL: WS_URL env var is required (e.g. ws://127.0.0.1:6381/ws/v1)");
  process.exit(1);
}

const enc = new TextEncoder();
const dec = new TextDecoder();

let failures = 0;
function check(name: string, ok: boolean, detail: string) {
  console.log(`${ok ? "PASS" : "FAIL"}: ${name}${ok ? "" : ` — ${detail}`}`);
  if (!ok) failures++;
}

// The command sequence: typed SET/GET/INCR, the generic escape hatch, and a
// ZADD/ZSCORE pair to prove double fidelity through the whole pipeline.
const PI = 3.141592653589793;
const commands: { name: string; cmd: Command; expect: (v: Value | undefined) => [boolean, string] }[] = [
  {
    name: "SET ts:k hello-ts (typed)",
    cmd: create(CommandSchema, {
      seq: 1n,
      cmd: { case: "set", value: { key: enc.encode("ts:k"), value: enc.encode("hello-ts") } },
    }),
    expect: (v) => [
      v?.kind.case === "simpleString" && v.kind.value === "OK",
      `want simpleString "OK", got ${JSON.stringify(v?.kind)}`,
    ],
  },
  {
    name: "GET ts:k (typed)",
    cmd: create(CommandSchema, {
      seq: 2n,
      cmd: { case: "get", value: { key: enc.encode("ts:k") } },
    }),
    expect: (v) => [
      v?.kind.case === "blobString" && dec.decode(v.kind.value) === "hello-ts",
      `want blobString "hello-ts", got ${v?.kind.case === "blobString" ? JSON.stringify(dec.decode(v.kind.value)) : JSON.stringify(v?.kind)}`,
    ],
  },
  {
    name: "generic set ts:n 40",
    cmd: create(CommandSchema, {
      seq: 3n,
      cmd: { case: "generic", value: { command: "set", args: [enc.encode("ts:n"), enc.encode("40")] } },
    }),
    expect: (v) => [
      v?.kind.case === "simpleString" && v.kind.value === "OK",
      `want simpleString "OK", got ${JSON.stringify(v?.kind)}`,
    ],
  },
  {
    name: "INCR ts:n by 5 (typed) → 45",
    cmd: create(CommandSchema, {
      seq: 4n,
      cmd: { case: "incr", value: { key: enc.encode("ts:n"), delta: 5n } },
    }),
    expect: (v) => [
      v?.kind.case === "int" && v.kind.value === 45n,
      `want int 45, got ${JSON.stringify(v?.kind, (_, x) => (typeof x === "bigint" ? Number(x) : x))}`,
    ],
  },
  {
    name: "generic GET ts:k",
    cmd: create(CommandSchema, {
      seq: 5n,
      cmd: { case: "generic", value: { command: "get", args: [enc.encode("ts:k")] } },
    }),
    expect: (v) => [
      v?.kind.case === "blobString" && dec.decode(v.kind.value) === "hello-ts",
      `want blobString "hello-ts", got ${v?.kind.case === "blobString" ? JSON.stringify(dec.decode(v.kind.value)) : JSON.stringify(v?.kind)}`,
    ],
  },
  {
    name: "ZADD ts:z pi=π (typed)",
    cmd: create(CommandSchema, {
      seq: 6n,
      cmd: {
        case: "zadd",
        value: { key: enc.encode("ts:z"), members: [{ score: PI, member: enc.encode("pi") }] },
      },
    }),
    expect: (v) => [
      v?.kind.case === "int" && v.kind.value === 1n,
      `want int 1 (added), got ${JSON.stringify(v?.kind, (_, x) => (typeof x === "bigint" ? Number(x) : x))}`,
    ],
  },
  {
    name: "ZSCORE ts:z pi (typed, double fidelity)",
    cmd: create(CommandSchema, {
      seq: 7n,
      cmd: { case: "zscore", value: { key: enc.encode("ts:z"), member: enc.encode("pi") } },
    }),
    expect: (v) => [
      v?.kind.case === "double" && v.kind.value === PI,
      `want double ${PI}, got ${v?.kind.case === "double" ? v.kind.value : JSON.stringify(v?.kind)}`,
    ],
  },
];

const ws = new WebSocket(WS_URL);
ws.binaryType = "arraybuffer";

let next = 0;
let done = false;

function finish(code: number) {
  if (done) return;
  done = true;
  clearTimeout(timer);
  try {
    ws.close();
  } catch {
    // already closed
  }
  console.log(code === 0 ? "PASS: all TS round-trip checks succeeded" : "FAIL: TS round-trip failed");
  process.exit(code);
}

const timer = setTimeout(() => {
  console.error(`FAIL: timed out after 10s waiting for reply ${next + 1}/${commands.length}`);
  finish(1);
}, 10_000);

ws.onopen = () => {
  for (const { cmd } of commands) {
    ws.send(toBinary(CommandSchema, cmd));
  }
};

ws.onmessage = (ev) => {
  if (typeof ev.data === "string") {
    console.error(`FAIL: unexpected text frame: ${ev.data}`);
    finish(1);
    return;
  }
  let resp;
  try {
    resp = fromBinary(CommandResponseSchema, new Uint8Array(ev.data as ArrayBuffer));
  } catch (err) {
    console.error(`FAIL: reply is not a CommandResponse: ${err}`);
    finish(1);
    return;
  }
  if (resp.seq === 0n) {
    // Unsolicited push frame; not expected in this script, but not fatal.
    console.log("note: ignoring unsolicited seq-0 push frame");
    return;
  }
  const idx = next;
  const want = commands[idx];
  if (!want) {
    console.error(`FAIL: unexpected extra reply seq=${resp.seq}`);
    finish(1);
    return;
  }
  check(`${want.name} — seq ${resp.seq}`, resp.seq === BigInt(idx + 1), `want seq ${idx + 1}`);
  const [ok, detail] = want.expect(resp.reply);
  check(`${want.name} — reply value`, ok, detail);
  next++;
  if (next === commands.length) finish(failures === 0 ? 0 : 1);
};

ws.onerror = (ev) => {
  console.error(`FAIL: WebSocket error: ${ev}`);
  finish(1);
};

ws.onclose = (ev) => {
  if (!done) {
    console.error(`FAIL: connection closed before all replies arrived (code=${ev.code} reason=${ev.reason})`);
    finish(1);
  }
};
