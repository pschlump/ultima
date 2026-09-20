// UltimaWS: WebSocket client for /ws/v1 (design doc §6.3, D15) with the
// §9.4 resumable-session protocol (D18).
//
// Wire: one binary protobuf Command frame per command; replies are
// CommandResponse frames correlated by seq. Pub/sub (and MONITOR) pushes
// arrive as unsolicited seq-0 frames carrying a push Value; on a sessioned
// connection they are stamped with push_seq.
//
// Session handshake: the first frame on a connection is an empty Command
// (no cmd oneof, seq of our choice) to request a fresh session — the reply
// carries the new session id. On reconnect the first frame is
// Command{session, last_push_seq} to resume; a failed resume is an error
// reply starting with SESSION_EXPIRED — the session is dropped, a fresh
// handshake is sent, and onSessionReset fires so screens re-subscribe.
// Command replies lost to a drop come back after resume as error frames
// starting with ABORTED, carrying the original seq.
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import {
  CommandResponseSchema,
  CommandSchema,
  type Command,
  type CommandResponse,
  type Value,
} from "../../../gen/ts/ultima/v1/command_pb";

export type WSState = "connecting" | "open" | "reconnecting" | "closed";

export interface UltimaWSHandlers {
  /** Unsolicited seq-0 push frames (pub/sub messages, subscribe acks, MONITOR lines). */
  onPush?: (value: Value, pushSeq: bigint) => void;
  /** The resume failed with SESSION_EXPIRED: subscriptions were lost; re-subscribe. */
  onSessionReset?: () => void;
  /** A previous session was resumed successfully — subscriptions were retained. */
  onResumed?: () => void;
  onStateChange?: (state: WSState) => void;
}

interface Pending {
  resolve: (v: Value) => void;
  reject: (e: Error) => void;
}

const SESSION_KEY = "ultima.wsSession";
const enc = new TextEncoder();

function loadStoredSession(): { id: string; lastPushSeq: bigint } | null {
  try {
    const raw = localStorage.getItem(SESSION_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as { id?: unknown; lastPushSeq?: unknown };
    if (typeof parsed.id !== "string" || parsed.id === "") return null;
    const seq = typeof parsed.lastPushSeq === "string" ? BigInt(parsed.lastPushSeq) : 0n;
    return { id: parsed.id, lastPushSeq: seq };
  } catch {
    return null;
  }
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

  constructor(
    private readonly getToken: () => string | null,
    private readonly handlers: UltimaWSHandlers = {},
    private readonly useSession = true,
  ) {
    const stored = useSession ? loadStoredSession() : null;
    if (stored) {
      this.sessionId = stored.id;
      this.lastPushSeq = stored.lastPushSeq;
    }
  }

  get state(): WSState {
    if (this.wantClose) return "closed";
    if (this.ws && this.ws.readyState === WebSocket.OPEN && this.handshaken) return "open";
    if (this.reconnectAttempt > 0) return "reconnecting";
    return "connecting";
  }

  get session(): string | null {
    return this.sessionId;
  }

  private setState(): void {
    this.handlers.onStateChange?.(this.state);
  }

  /** Connect (or reconnect after close()). No-op if already connected. */
  connect(): void {
    if (this.ws || this.wantClose) return;
    const token = this.getToken();
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const url = `${proto}://${location.host}/ws/v1${token ? `?access_token=${encodeURIComponent(token)}` : ""}`;
    const ws = new WebSocket(url);
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
      let resp;
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
      // Reject only commands that were never sent; in-flight ones will be
      // resolved by the resume (or ABORTED frames) after reconnect.
      const delay = Math.min(10_000, 500 * 2 ** this.reconnectAttempt);
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

  private encode(cmd: Command): Uint8Array {
    return toBinary(CommandSchema, cmd);
  }

  private sendHandshake(): void {
    if (!this.useSession) {
      // Sessionless connections behave exactly as in M4: no handshake.
      this.handshaken = true;
      this.flush();
      this.setState();
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
    this.ws?.send(this.encode(cmd));
  }

  private flush(): void {
    const queued = this.outbox;
    this.outbox = [];
    for (const frame of queued) this.ws?.send(frame);
  }

  private onFrame(resp: CommandResponse): void {
    if (resp.seq === 0n) {
      // Unsolicited push frame.
      if (resp.pushSeq > this.lastPushSeq) {
        this.lastPushSeq = resp.pushSeq;
        if (this.sessionId) this.persistSession();
      }
      if (resp.reply) this.handlers.onPush?.(resp.reply, resp.pushSeq);
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
          // Drop the session and start fresh; screens re-subscribe.
          this.sessionId = null;
          this.lastPushSeq = 0n;
          localStorage.removeItem(SESSION_KEY);
          this.handshaken = false;
          this.sendHandshake();
          this.handlers.onSessionReset?.();
          return;
        }
        // e.g. sessions not enabled on this server — run sessionless.
        this.sessionId = null;
        this.flush();
        this.setState();
        return;
      }
      if (resp.session) {
        const wasResume = this.sessionId === resp.session && this.sessionId !== null;
        this.sessionId = resp.session;
        this.persistSession();
        if (wasResume) this.handlers.onResumed?.();
      }
      this.flush();
      this.setState();
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

  private persistSession(): void {
    if (!this.sessionId) return;
    try {
      localStorage.setItem(
        SESSION_KEY,
        JSON.stringify({ id: this.sessionId, lastPushSeq: this.lastPushSeq.toString() }),
      );
    } catch {
      // storage full/unavailable — session resume just won't survive reload
    }
  }

  /**
   * Run one generic command; resolves with the reply Value (error replies
   * resolve too — the caller inspects the kind, like a RESP client).
   */
  exec(command: string, args: string[]): Promise<Value> {
    if (this.wantClose) return Promise.reject(new Error("connection closed"));
    const seq = this.nextSeq();
    const cmd = create(CommandSchema, {
      seq,
      cmd: {
        case: "generic",
        value: { command, args: args.map((a) => enc.encode(a)) },
      },
    });
    const frame = this.encode(cmd);
    return new Promise<Value>((resolve, reject) => {
      this.pending.set(seq, { resolve, reject });
      this.sendRaw(frame);
    });
  }
}
