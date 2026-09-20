// Reply-value helpers for the ultima.v1.Value protobuf mirror of the RESP3
// type system (proto/ultima/v1/command.proto). The typed UltimaWS helpers
// unwrap replies through these; the generic exec() returns the raw Value.
import type { Value } from "../../../gen/ts/ultima/v1/command_pb";

/** A command reply whose kind is "error" (the RESP3 - reply). */
export class ReplyError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ReplyError";
  }
}

const dec = new TextDecoder();

/** Throws ReplyError on error replies; passes every other kind through. */
export function checkError(v: Value): Value {
  if (v.kind.case === "error") throw new ReplyError(v.kind.value);
  return v;
}

export function isNull(v: Value): boolean {
  return v.kind.case === "null";
}

/** UTF-8 string view of scalar replies; null for the RESP3 null reply. */
export function asString(v: Value): string | null {
  checkError(v);
  switch (v.kind.case) {
    case "null":
      return null;
    case "blobString":
      return dec.decode(v.kind.value);
    case "simpleString":
    case "bigNumber":
      return v.kind.value;
    case "verbatim":
      return dec.decode(v.kind.value.payload);
    case "int":
      return v.kind.value.toString();
    case "double":
      return String(v.kind.value);
    case "bool":
      return v.kind.value ? "1" : "0";
    default:
      throw new ReplyError(`unexpected aggregate reply type "${v.kind.case ?? "unset"}"`);
  }
}

/** Raw bytes of scalar replies; null for the RESP3 null reply. */
export function asBytes(v: Value): Uint8Array | null {
  checkError(v);
  switch (v.kind.case) {
    case "null":
      return null;
    case "blobString":
      return v.kind.value;
    case "verbatim":
      return v.kind.value.payload;
    default: {
      const s = asString(v);
      return s === null ? null : new TextEncoder().encode(s);
    }
  }
}

/** Integer replies (bigint, exact); numeric strings are parsed. */
export function asBigInt(v: Value): bigint {
  checkError(v);
  if (v.kind.case === "int") return v.kind.value;
  const s = asString(v);
  if (s === null) throw new ReplyError("expected integer reply, got null");
  try {
    return BigInt(s);
  } catch {
    throw new ReplyError(`expected integer reply, got "${s}"`);
  }
}

/** Integer replies narrowed to number (safe for counts/lengths). */
export function asInt(v: Value): number {
  return Number(asBigInt(v));
}

/** Double replies; integer and numeric-string replies are coerced. */
export function asDouble(v: Value): number | null {
  checkError(v);
  if (v.kind.case === "null") return null;
  if (v.kind.case === "double") return v.kind.value;
  if (v.kind.case === "int") return Number(v.kind.value);
  const s = asString(v);
  if (s === null) return null;
  const n = Number(s);
  if (Number.isNaN(n) && s !== "nan" && s !== "-nan") {
    throw new ReplyError(`expected double reply, got "${s}"`);
  }
  return n;
}

/** Boolean view: RESP3 bool, or integer 0/1 (Redis-style flags). */
export function asBool(v: Value): boolean {
  checkError(v);
  if (v.kind.case === "bool") return v.kind.value;
  return asBigInt(v) !== 0n;
}

function elements(v: Value): Value[] {
  checkError(v);
  switch (v.kind.case) {
    case "array":
    case "set":
    case "push":
      return v.kind.value.elems;
    default:
      throw new ReplyError(`expected array reply, got "${v.kind.case ?? "unset"}"`);
  }
}

/** Array/set replies as strings; null elements stay null (e.g. MGET misses). */
export function asStringArray(v: Value): (string | null)[] {
  return elements(v).map(asString);
}

/** Map replies (HGETALL), accepting the RESP2 flat-array shape too. */
export function asStringMap(v: Value): Record<string, string> {
  checkError(v);
  const out: Record<string, string> = {};
  if (v.kind.case === "map") {
    for (const p of v.kind.value.pairs) {
      const k = p.key ? asString(p.key) : null;
      const val = p.value ? asString(p.value) : null;
      if (k !== null && val !== null) out[k] = val;
    }
    return out;
  }
  const flat = elements(v);
  for (let i = 0; i + 1 < flat.length; i += 2) {
    const k = asString(flat[i]!);
    const val = asString(flat[i + 1]!);
    if (k !== null && val !== null) out[k] = val;
  }
  return out;
}

/** ZRANGE … WITHSCORES replies: RESP3 nests [member, score] pairs, RESP2 is
 * a flat alternating array — accept both. */
export function asScoredMembers(v: Value): { member: string; score: number }[] {
  const flat = elements(v);
  const out: { member: string; score: number }[] = [];
  if (flat.every((e) => e.kind.case === "array" || e.kind.case === "set")) {
    for (const pair of flat) {
      const elems = elements(pair);
      const member = elems[0] ? asString(elems[0]) : null;
      const score = elems[1] ? asDouble(elems[1]) : null;
      if (member !== null && score !== null) out.push({ member, score });
    }
    return out;
  }
  for (let i = 0; i + 1 < flat.length; i += 2) {
    const member = asString(flat[i]!);
    const score = asDouble(flat[i + 1]!);
    if (member !== null && score !== null) out.push({ member, score });
  }
  return out;
}
