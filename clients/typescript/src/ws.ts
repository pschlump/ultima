// UltimaWS: framework-agnostic WebSocket client for /ws/v1 (design doc
// §6.3, D15) with the §9.4 resumable-session protocol (D18). Generalized
// from the web UI's web/src/lib/ws.ts: the URL, token, storage, and
// reconnect policy are injected, typed command helpers ride the protobuf
// oneof, and subscriptions are tracked client-side so they are transparently
// re-issued whenever the server lost them (fresh handshake after a drop or
// a SESSION_EXPIRED gap).
//
// Wire: one binary protobuf Command frame per command; replies are
// CommandResponse frames correlated by seq. Pub/sub pushes arrive as
// unsolicited seq-0 frames carrying a push Value; on a sessioned connection
// they are stamped with push_seq.
//
// Session handshake: the first frame on a connection is an empty Command
// (fresh session) or Command{session, last_push_seq} (resume). A failed
// resume is an error reply starting with SESSION_EXPIRED — the session is
// dropped, onGap fires, a fresh handshake follows, and subscriptions are
// re-issued. Command replies lost to a drop come back after a successful
// resume as error frames starting with ABORTED, failing the matching
// pending call.
import { create, fromBinary, toBinary, type MessageInitShape } from "@bufbuild/protobuf";
import {
  CommandResponseSchema,
  CommandSchema,
  type CommandResponse,
  type Value,
} from "../../../gen/ts/ultima/v1/command_pb";
import { defaultStorage, type StorageLike } from "./storage";
import {
  asBigInt,
  asBool,
  asDouble,
  asInt,
  asScoredMembers,
  asString,
  asStringArray,
  asStringMap,
  checkError,
} from "./value";

export type WSState = "connecting" | "open" | "reconnecting" | "closed";

/** One pub/sub delivery. `pattern` is set on pmessage deliveries. */
export interface PubSubMessage {
  channel: string;
  payload: Uint8Array;
  /** UTF-8 view of payload. */
  text: string;
  pattern?: string;
  pushSeq: bigint;
}

export type PushHandler = (msg: PubSubMessage) => void;

export interface ReconnectOptions {
  /** First retry delay; doubles per attempt up to maxDelayMs. Default 500. */
  baseDelayMs?: number;
  /** Default 10000. */
  maxDelayMs?: number;
}

export interface UltimaWSOptions {
  /** ws(s):// URL of the /ws/v1 endpoint. */
  url: string;
  /** Bearer access token for the upgrade (?access_token=). Called per dial. */
  getToken?: () => string | null;
  /** Awaited before every dial — e.g. proactive token refresh. */
  beforeConnect?: () => unknown | Promise<unknown>;
  /** Resumable sessions (§9.4); default true. False = M4 sessionless mode. */
  session?: boolean;
  /** Seed a session to resume (overrides any stored session). */
  resume?: { id: string; lastPushSeq?: bigint } | null;
  /** Session-id persistence; defaults to localStorage or in-memory. */
  storage?: StorageLike;
  storageKey?: string;
  reconnect?: ReconnectOptions;
  /** Every unsolicited seq-0 push frame, raw (pub/sub, MONITOR lines). */
  onPush?: (value: Value, pushSeq: bigint) => void;
  /** The session expired unrecoverably (SESSION_EXPIRED): pushes were lost. */
  onGap?: (sessionId: string) => void;
  /** A previous session was resumed; subscriptions were retained. */
  onResumed?: (sessionId: string) => void;
  onStateChange?: (state: WSState) => void;
  /** Background error sink (re-subscribe failures, prepare errors). */
  onError?: (err: Error) => void;
  /** Injectable for exotic runtimes; defaults to the global WebSocket. */
  WebSocketImpl?: typeof WebSocket;
}

interface Pending {
  resolve: (v: Value) => void;
  reject: (e: Error) => void;
}

interface SubEntry {
  pattern: boolean;
  name: string;
  handlers: Set<PushHandler>;
}

export interface SetOptions {
  /** Only set when the key does not exist. */
  nx?: boolean;
  /** Only set when the key exists. */
  xx?: boolean;
  /** Return the old value instead of "OK". */
  get?: boolean;
  /** Expiry in milliseconds (PX). */
  px?: number;
}

export interface ZAddOptions {
  nx?: boolean;
  xx?: boolean;
  gt?: boolean;
  lt?: boolean;
  ch?: boolean;
}

/** string keys/values are UTF-8 encoded; Uint8Array passes through. */
export type Bytes = string | Uint8Array;

const enc = new TextEncoder();
const dec = new TextDecoder();

function b(v: Bytes): Uint8Array {
  return typeof v === "string" ? enc.encode(v) : v;
}

function pairsOf(p: Record<string, string> | Iterable<readonly [string, string]>): [string, string][] {
  if (p instanceof Map) return [...p.entries()].map(([k, v]) => [k, v]);
  if (Array.isArray(p)) return p.map(([k, v]) => [k, v]);
  if (typeof p === "object" && p !== null && Symbol.iterator in p) {
    return [...(p as Iterable<readonly [string, string]>)].map(([k, v]) => [k, v]);
  }
  return Object.entries(p as Record<string, string>);
}

function subKey(pattern: boolean, name: string): string {
  return (pattern ? "p:" : "c:") + name;
}

export class UltimaWS {
  private ws: WebSocket | null = null;
  private seq = 0n;
  private readonly pending = new Map<bigint, Pending>();
  /** Frames encoded while the socket was down; flushed after the handshake. */
  private outbox: Uint8Array[] = [];
  private sessionId: string | null = null;
  private lastPushSeq = 0n;
  private wantClose = false;
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  /** True once the handshake reply for the current socket has been processed. */
  private handshaken = false;
  private handshakePending = 0;
  /** True once any connection completed a handshake (drives re-subscription). */
  private connectedOnce = false;
  private dialing = false;
  private readonly subs = new Map<string, SubEntry>();
  private readonly stateListeners = new Set<(s: WSState) => void>();
  private readonly storage: StorageLike;
  private readonly storageKey: string;
  private readonly useSession: boolean;
  private readonly WSImpl: typeof WebSocket;

  constructor(private readonly options: UltimaWSOptions) {
    this.useSession = options.session !== false;
    this.storage = options.storage ?? defaultStorage();
    this.storageKey = options.storageKey ?? "ultima.wsSession";
    this.WSImpl = options.WebSocketImpl ?? WebSocket;
    const seed = this.useSession ? (options.resume ?? this.loadStoredSession()) : null;
    if (seed) {
      this.sessionId = seed.id;
      this.lastPushSeq = seed.lastPushSeq ?? 0n;
    }
  }

  get state(): WSState {
    if (this.wantClose) return "closed";
    if (this.ws && this.ws.readyState === WebSocket.OPEN && this.handshaken) return "open";
    if (this.reconnectAttempt > 0 || this.connectedOnce) return "reconnecting";
    return "connecting";
  }

  /** The live session id, or null when sessionless / not yet handshaken. */
  get session(): string | null {
    return this.sessionId;
  }

  /** Highest push_seq seen; presented as last_push_seq on resume. */
  get pushSeq(): bigint {
    return this.lastPushSeq;
  }

  /** Register an additional state-change listener; returns an unsubscribe. */
  addStateListener(fn: (s: WSState) => void): () => void {
    this.stateListeners.add(fn);
    return () => this.stateListeners.delete(fn);
  }

  /** Resolves once the connection is open and handshaken. */
  ready(timeoutMs = 10_000): Promise<void> {
    if (this.state === "open") return Promise.resolve();
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        off();
        reject(new Error("UltimaWS: timed out waiting for the connection"));
      }, timeoutMs);
      const off = this.addStateListener((s) => {
        if (s === "open") {
          clearTimeout(timer);
          off();
          resolve();
        } else if (s === "closed") {
          clearTimeout(timer);
          off();
          reject(new Error("UltimaWS: connection closed"));
        }
      });
    });
  }

  /** Connect (no-op while a socket exists). Reconnects are automatic. */
  connect(): void {
    if (this.ws || this.dialing || this.wantClose) return;
    this.dialing = true;
    void Promise.resolve(this.options.beforeConnect?.())
      .catch((err: unknown) => {
        this.options.onError?.(err instanceof Error ? err : new Error(String(err)));
      })
      .finally(() => {
        this.dialing = false;
        if (this.ws || this.wantClose) return;
        this.dial();
      });
  }

  private dial(): void {
    const token = this.options.getToken?.() ?? null;
    const url = this.options.url + (token ? `?access_token=${encodeURIComponent(token)}` : "");
    const ws = new this.WSImpl(url);
    ws.binaryType = "arraybuffer";
    this.ws = ws;
    this.handshaken = false;

    ws.onopen = () => {
      this.reconnectAttempt = 0;
      this.sendHandshake();
      this.setState();
    };
    ws.onmessage = (ev) => {
      if (typeof ev.data === "string") return;
      let resp: CommandResponse;
      try {
        resp = fromBinary(CommandResponseSchema, new Uint8Array(ev.data as ArrayBuffer));
      } catch {
        return;
      }
      this.onFrame(resp);
    };
    ws.onclose = () => {
      this.ws = null;
      if (this.wantClose) {
        this.setState();
        return;
      }
      // In-flight commands stay pending: after a successful resume they are
      // resolved by ABORTED frames; unsent frames sit in the outbox.
      const base = this.options.reconnect?.baseDelayMs ?? 500;
      const max = this.options.reconnect?.maxDelayMs ?? 10_000;
      const delay = Math.min(max, base * 2 ** this.reconnectAttempt);
      this.reconnectAttempt++;
      this.setState();
      this.reconnectTimer = setTimeout(() => this.connect(), delay);
    };
    ws.onerror = () => {
      // onclose follows; reconnect logic lives there.
    };
    this.setState();
  }

  /** Permanently close: no reconnect, pending commands rejected. */
  close(): void {
    this.wantClose = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.ws?.close();
    for (const [, p] of this.pending) p.reject(new Error("connection closed"));
    this.pending.clear();
    this.outbox = [];
    this.setState();
  }

  /**
   * Simulate a network drop: closes the socket without marking the client
   * closed, so the §9.4 resume path runs on the automatic reconnect.
   * Intended for tests and demos.
   */
  forceReconnect(): void {
    this.ws?.close();
  }

  private setState(): void {
    const s = this.state;
    this.options.onStateChange?.(s);
    for (const fn of this.stateListeners) fn(s);
  }

  private nextSeq(): bigint {
    this.seq += 1n;
    return this.seq;
  }

  private sendRaw(frame: Uint8Array): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(frame);
    } else {
      this.outbox.push(frame);
    }
  }

  private sendHandshake(): void {
    if (!this.useSession) {
      // Sessionless connections behave exactly as in M4: no handshake.
      this.handshaken = true;
      this.onHandshakeComplete(false);
      return;
    }
    this.handshakePending++;
    const cmd = this.sessionId
      ? create(CommandSchema, {
          seq: this.nextSeq(),
          session: this.sessionId,
          lastPushSeq: this.lastPushSeq,
        })
      : create(CommandSchema, { seq: this.nextSeq() });
    this.ws?.send(toBinary(CommandSchema, cmd));
  }

  private flush(): void {
    const queued = this.outbox;
    this.outbox = [];
    for (const frame of queued) this.ws?.send(frame);
  }

  /** Post-handshake bookkeeping: flush queued frames; re-subscribe when the
   * server lost our subscriptions (reconnect without a successful resume). */
  private onHandshakeComplete(resumed: boolean): void {
    const reconnect = this.connectedOnce;
    this.connectedOnce = true;
    this.flush();
    this.setState();
    if (reconnect && !resumed) {
      for (const e of this.subs.values()) {
        this.exec(e.pattern ? "psubscribe" : "subscribe", e.name).catch((err: unknown) => {
          this.options.onError?.(err instanceof Error ? err : new Error(String(err)));
        });
      }
    }
  }

  private onFrame(resp: CommandResponse): void {
    if (resp.seq === 0n) {
      // Unsolicited push frame.
      if (resp.pushSeq > this.lastPushSeq) {
        this.lastPushSeq = resp.pushSeq;
        if (this.sessionId) this.persistSession();
      }
      if (resp.reply) this.dispatchPush(resp.reply, resp.pushSeq);
      return;
    }
    const value = resp.reply;
    if (this.handshakePending > 0 && !this.handshaken) {
      // The handshake reply is the first non-push frame on a connection.
      this.handshakePending--;
      this.handshaken = true;
      if (value?.kind.case === "error") {
        const text = value.kind.value;
        if (text.startsWith("SESSION_EXPIRED")) {
          // Drop the session and start fresh; the gap is unrecoverable.
          const lost = this.sessionId ?? "";
          this.sessionId = null;
          this.lastPushSeq = 0n;
          this.storage.removeItem(this.storageKey);
          this.handshaken = false;
          this.sendHandshake();
          this.options.onGap?.(lost);
          return;
        }
        // e.g. sessions not enabled on this server — run sessionless.
        this.sessionId = null;
        this.onHandshakeComplete(false);
        return;
      }
      let resumed = false;
      if (resp.session) {
        resumed = this.sessionId === resp.session && this.sessionId !== null;
        this.sessionId = resp.session;
        this.persistSession();
        if (resumed) this.options.onResumed?.(resp.session);
      }
      this.onHandshakeComplete(resumed);
      return;
    }
    if (value?.kind.case === "error" && value.kind.value.startsWith("ABORTED")) {
      const p = this.pending.get(resp.seq);
      if (p) {
        this.pending.delete(resp.seq);
        p.reject(new Error(value.kind.value));
      }
      return;
    }
    const p = this.pending.get(resp.seq);
    if (p) {
      this.pending.delete(resp.seq);
      if (value) p.resolve(value);
      else p.reject(new Error("empty reply"));
    }
  }

  private dispatchPush(v: Value, pushSeq: bigint): void {
    this.options.onPush?.(v, pushSeq);
    if (v.kind.case !== "push") return;
    const elems = v.kind.value.elems;
    const kind = elems[0] ? asString(elems[0]) : null;
    if (kind === "message" && elems.length >= 3) {
      const channel = elems[1] ? asString(elems[1]) : null;
      const payload = elems[2];
      if (channel === null || !payload) return;
      this.deliver(subKey(false, channel), channel, payload, pushSeq, undefined);
    } else if (kind === "pmessage" && elems.length >= 4) {
      const pattern = elems[1] ? asString(elems[1]) : null;
      const channel = elems[2] ? asString(elems[2]) : null;
      const payload = elems[3];
      if (pattern === null || channel === null || !payload) return;
      this.deliver(subKey(true, pattern), channel, payload, pushSeq, pattern);
    }
    // (p)subscribe/(p)unsubscribe acks also arrive as command replies for
    // the exec() that requested them; nothing to dispatch here.
  }

  private deliver(key: string, channel: string, payload: Value, pushSeq: bigint, pattern: string | undefined): void {
    const entry = this.subs.get(key);
    if (!entry) return;
    const bytes = payload.kind.case === "blobString" ? payload.kind.value : enc.encode(asString(payload) ?? "");
    const msg: PubSubMessage = {
      channel,
      payload: bytes,
      text: dec.decode(bytes),
      pushSeq,
      ...(pattern !== undefined ? { pattern } : {}),
    };
    for (const h of entry.handlers) {
      try {
        h(msg);
      } catch (err) {
        this.options.onError?.(err instanceof Error ? err : new Error(String(err)));
      }
    }
  }

  private loadStoredSession(): { id: string; lastPushSeq: bigint } | null {
    try {
      const raw = this.storage.getItem(this.storageKey);
      if (!raw) return null;
      const parsed = JSON.parse(raw) as { id?: unknown; lastPushSeq?: unknown };
      if (typeof parsed.id !== "string" || parsed.id === "") return null;
      const seq = typeof parsed.lastPushSeq === "string" ? BigInt(parsed.lastPushSeq) : 0n;
      return { id: parsed.id, lastPushSeq: seq };
    } catch {
      return null;
    }
  }

  private persistSession(): void {
    if (!this.sessionId) return;
    try {
      this.storage.setItem(
        this.storageKey,
        JSON.stringify({ id: this.sessionId, lastPushSeq: this.lastPushSeq.toString() }),
      );
    } catch {
      // storage full/blocked — resume just won't survive a reload
    }
  }

  // -----------------------------------------------------------------------
  // Commands
  // -----------------------------------------------------------------------

  private run(cmd: NonNullable<MessageInitShape<typeof CommandSchema>["cmd"]>): Promise<Value> {
    if (this.wantClose) return Promise.reject(new Error("connection closed"));
    const seq = this.nextSeq();
    const frame = toBinary(CommandSchema, create(CommandSchema, { seq, cmd }));
    return new Promise<Value>((resolve, reject) => {
      this.pending.set(seq, { resolve, reject });
      this.sendRaw(frame);
    });
  }

  /**
   * Generic escape hatch: run any command by name. Error replies RESOLVE
   * (inspect the Value kind, like a RESP client); the typed helpers below
   * throw ReplyError on them instead.
   */
  exec(command: string, ...args: Bytes[]): Promise<Value> {
    return this.run({ case: "generic", value: { command, args: args.map(b) } });
  }

  ping(message?: Bytes): Promise<string> {
    const v = message === undefined ? this.exec("ping") : this.exec("ping", message);
    return v.then((r) => asString(r) ?? "");
  }

  async set(key: Bytes, value: Bytes, options: SetOptions & { get: true }): Promise<string | null>;
  async set(key: Bytes, value: Bytes, options?: SetOptions): Promise<"OK" | null>;
  async set(key: Bytes, value: Bytes, options?: SetOptions): Promise<string | null> {
    const v = await this.run({
      case: "set",
      value: {
        key: b(key),
        value: b(value),
        ttlMs: BigInt(options?.px ?? 0),
        nx: options?.nx ?? false,
        xx: options?.xx ?? false,
        get: options?.get ?? false,
      },
    });
    return asString(v);
  }

  async get(key: Bytes): Promise<string | null> {
    return asString(await this.run({ case: "get", value: { key: b(key) } }));
  }

  async getBytes(key: Bytes): Promise<Uint8Array | null> {
    const v = checkError(await this.run({ case: "get", value: { key: b(key) } }));
    if (v.kind.case === "null") return null;
    if (v.kind.case === "blobString") return v.kind.value;
    const s = asString(v);
    return s === null ? null : enc.encode(s);
  }

  async del(...keys: Bytes[]): Promise<number> {
    return asInt(await this.run({ case: "del", value: { keys: keys.map(b) } }));
  }

  async exists(...keys: Bytes[]): Promise<number> {
    return asInt(await this.run({ case: "exists", value: { keys: keys.map(b) } }));
  }

  async incrBy(key: Bytes, delta: bigint | number): Promise<bigint> {
    return asBigInt(
      await this.run({ case: "incr", value: { key: b(key), delta: BigInt(delta) } }),
    );
  }

  incr(key: Bytes): Promise<bigint> {
    return this.incrBy(key, 1n);
  }

  decr(key: Bytes): Promise<bigint> {
    return this.incrBy(key, -1n);
  }

  async incrByFloat(key: Bytes, delta: number): Promise<number> {
    const v = await this.run({ case: "incrFloat", value: { key: b(key), delta } });
    const d = asDouble(v);
    if (d === null) throw new Error("INCRBYFLOAT: unexpected null reply");
    return d;
  }

  async mget(...keys: Bytes[]): Promise<(string | null)[]> {
    return asStringArray(await this.run({ case: "mget", value: { keys: keys.map(b) } }));
  }

  async mset(pairs: Record<string, string> | Iterable<readonly [string, string]>): Promise<void> {
    const v = await this.run({
      case: "mset",
      value: { pairs: pairsOf(pairs).map(([key, value]) => ({ key: enc.encode(key), value: enc.encode(value) })) },
    });
    checkError(v);
  }

  async append(key: Bytes, value: Bytes): Promise<number> {
    return asInt(await this.run({ case: "append", value: { key: b(key), value: b(value) } }));
  }

  /** PEXPIRE semantics (millisecond ttl). True when the expiry was set. */
  async pexpire(key: Bytes, ttlMs: number): Promise<boolean> {
    return asBool(await this.run({ case: "expire", value: { key: b(key), ttlMs: BigInt(ttlMs) } }));
  }

  /** PTTL semantics: ms to live, -1 = no expiry, -2 = no such key. */
  async pttl(key: Bytes): Promise<number> {
    return asInt(await this.run({ case: "ttl", value: { key: b(key) } }));
  }

  async persist(key: Bytes): Promise<boolean> {
    return asBool(await this.run({ case: "persist", value: { key: b(key) } }));
  }

  async hget(key: Bytes, field: Bytes): Promise<string | null> {
    return asString(await this.run({ case: "hget", value: { key: b(key), field: b(field) } }));
  }

  async hset(key: Bytes, pairs: Record<string, string> | Iterable<readonly [string, string]>): Promise<number> {
    return asInt(
      await this.run({
        case: "hset",
        value: {
          key: b(key),
          pairs: pairsOf(pairs).map(([field, value]) => ({ field: enc.encode(field), value: enc.encode(value) })),
        },
      }),
    );
  }

  async hgetall(key: Bytes): Promise<Record<string, string>> {
    return asStringMap(await this.run({ case: "hgetall", value: { key: b(key) } }));
  }

  async hdel(key: Bytes, ...fields: Bytes[]): Promise<number> {
    return asInt(await this.run({ case: "hdel", value: { key: b(key), fields: fields.map(b) } }));
  }

  async hincrBy(key: Bytes, field: Bytes, delta: bigint | number): Promise<bigint> {
    return asBigInt(
      await this.run({ case: "hincrby", value: { key: b(key), field: b(field), delta: BigInt(delta) } }),
    );
  }

  async lpush(key: Bytes, ...elems: Bytes[]): Promise<number> {
    return asInt(await this.run({ case: "lpush", value: { key: b(key), elems: elems.map(b) } }));
  }

  async rpush(key: Bytes, ...elems: Bytes[]): Promise<number> {
    return asInt(await this.run({ case: "rpush", value: { key: b(key), elems: elems.map(b) } }));
  }

  async lpop(key: Bytes): Promise<string | null> {
    return asString(await this.run({ case: "lpop", value: { key: b(key) } }));
  }

  async rpop(key: Bytes): Promise<string | null> {
    return asString(await this.run({ case: "rpop", value: { key: b(key) } }));
  }

  async lrange(key: Bytes, start: number, stop: number): Promise<string[]> {
    const v = await this.run({ case: "lrange", value: { key: b(key), start: BigInt(start), stop: BigInt(stop) } });
    return asStringArray(v).map((s) => s ?? "");
  }

  async llen(key: Bytes): Promise<number> {
    return asInt(await this.run({ case: "llen", value: { key: b(key) } }));
  }

  async sadd(key: Bytes, ...members: Bytes[]): Promise<number> {
    return asInt(await this.run({ case: "sadd", value: { key: b(key), members: members.map(b) } }));
  }

  async srem(key: Bytes, ...members: Bytes[]): Promise<number> {
    return asInt(await this.run({ case: "srem", value: { key: b(key), members: members.map(b) } }));
  }

  async smembers(key: Bytes): Promise<string[]> {
    const v = await this.run({ case: "smembers", value: { key: b(key) } });
    return asStringArray(v).map((s) => s ?? "");
  }

  async sismember(key: Bytes, member: Bytes): Promise<boolean> {
    return asBool(await this.run({ case: "sismember", value: { key: b(key), member: b(member) } }));
  }

  async zadd(
    key: Bytes,
    members: Record<string, number> | Iterable<readonly [string, number]>,
    options?: ZAddOptions,
  ): Promise<number> {
    const list: readonly (readonly [string, number])[] =
      typeof members === "object" && members !== null && Symbol.iterator in members
        ? [...(members as Iterable<readonly [string, number]>)]
        : Object.entries(members as Record<string, number>);
    return asInt(
      await this.run({
        case: "zadd",
        value: {
          key: b(key),
          members: list.map(([member, score]) => ({ score, member: enc.encode(member) })),
          nx: options?.nx ?? false,
          xx: options?.xx ?? false,
          gt: options?.gt ?? false,
          lt: options?.lt ?? false,
          ch: options?.ch ?? false,
          incr: false,
        },
      }),
    );
  }

  async zscore(key: Bytes, member: Bytes): Promise<number | null> {
    return asDouble(await this.run({ case: "zscore", value: { key: b(key), member: b(member) } }));
  }

  async zrange(key: Bytes, start: number, stop: number): Promise<string[]> {
    const v = await this.run({
      case: "zrange",
      value: { key: b(key), start: BigInt(start), stop: BigInt(stop), withscores: false },
    });
    return asStringArray(v).map((s) => s ?? "");
  }

  async zrangeWithScores(key: Bytes, start: number, stop: number): Promise<{ member: string; score: number }[]> {
    const v = await this.run({
      case: "zrange",
      value: { key: b(key), start: BigInt(start), stop: BigInt(stop), withscores: true },
    });
    return asScoredMembers(v);
  }

  async zrem(key: Bytes, ...members: Bytes[]): Promise<number> {
    return asInt(await this.run({ case: "zrem", value: { key: b(key), members: members.map(b) } }));
  }

  async zcard(key: Bytes): Promise<number> {
    return asInt(await this.run({ case: "zcard", value: { key: b(key) } }));
  }

  // -----------------------------------------------------------------------
  // Pub/sub
  // -----------------------------------------------------------------------

  /**
   * Subscribe to a channel; resolves on the server ack. The subscription is
   * tracked client-side and transparently re-issued whenever a reconnect
   * could not resume the session (the server lost it). Use psubscribe() for
   * glob patterns.
   */
  async subscribe(channel: string, handler: PushHandler): Promise<void> {
    await this.addSub(false, channel, handler);
  }

  async psubscribe(pattern: string, handler: PushHandler): Promise<void> {
    await this.addSub(true, pattern, handler);
  }

  private async addSub(pattern: boolean, name: string, handler: PushHandler): Promise<void> {
    const key = subKey(pattern, name);
    let entry = this.subs.get(key);
    const first = !entry;
    if (!entry) {
      entry = { pattern, name, handlers: new Set() };
      this.subs.set(key, entry);
    }
    entry.handlers.add(handler);
    if (first) {
      // Resolves on the subscribe ack (queued while the socket is down).
      await this.exec(pattern ? "psubscribe" : "subscribe", name);
    }
  }

  /** Unsubscribe; with no handler, all handlers for the channel are removed. */
  async unsubscribe(channel: string, handler?: PushHandler): Promise<void> {
    await this.removeSub(false, channel, handler);
  }

  async punsubscribe(pattern: string, handler?: PushHandler): Promise<void> {
    await this.removeSub(true, pattern, handler);
  }

  private async removeSub(pattern: boolean, name: string, handler?: PushHandler): Promise<void> {
    const key = subKey(pattern, name);
    const entry = this.subs.get(key);
    if (!entry) return;
    if (handler) entry.handlers.delete(handler);
    if (handler && entry.handlers.size > 0) return;
    this.subs.delete(key);
    await this.exec(pattern ? "punsubscribe" : "unsubscribe", name);
  }
}
