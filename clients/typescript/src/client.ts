// UltimaClient: the umbrella tying the three pieces together (§11.2) —
// AuthManager (token lifecycle), RestClient (management API), and UltimaWS
// (/ws/v1 commands + pub/sub with §9.4 resumable sessions). Construct with a
// baseUrl or host/httpPort; the WS URL defaults to the same origin.
import { AuthManager, type Credentials, type SessionInfo } from "./auth";
import { RestClient } from "./rest";
import { defaultStorage, type StorageLike } from "./storage";
import { UltimaWS, type ReconnectOptions, type WSState } from "./ws";
import type { PushHandler } from "./ws";
import type { Value } from "../../../gen/ts/ultima/v1/command_pb";

export interface UltimaClientWSOptions {
  /** Override the derived ws(s)://<baseUrl>/ws/v1 endpoint. */
  url?: string;
  /** Resumable sessions; default true. */
  session?: boolean;
  resume?: { id: string; lastPushSeq?: bigint } | null;
  reconnect?: ReconnectOptions;
  onPush?: (value: Value, pushSeq: bigint) => void;
  onGap?: (sessionId: string) => void;
  onResumed?: (sessionId: string) => void;
  onStateChange?: (state: WSState) => void;
  onError?: (err: Error) => void;
  WebSocketImpl?: typeof WebSocket;
}

export interface UltimaClientOptions {
  /** e.g. "http://127.0.0.1:6381". Wins over host/httpPort. */
  baseUrl?: string;
  host?: string;
  /** Default 6381. */
  httpPort?: number;
  /** https/wss when true. Default false. */
  secure?: boolean;
  /** Automatic login (and re-login after refresh-token expiry). */
  credentials?: Credentials;
  /** Fired when a token refresh fails and the session is dropped. */
  onSessionExpired?: () => void;
  /** Shared storage for tokens and the WS session id. */
  storage?: StorageLike;
  ws?: UltimaClientWSOptions;
}

export class UltimaClient {
  readonly auth: AuthManager;
  readonly rest: RestClient;
  readonly ws: UltimaWS;
  readonly baseUrl: string;

  constructor(options: UltimaClientOptions = {}) {
    this.baseUrl =
      options.baseUrl ??
      `${options.secure ? "https" : "http"}://${options.host ?? "127.0.0.1"}:${options.httpPort ?? 6381}`;
    const storage = options.storage ?? defaultStorage();
    this.auth = new AuthManager({
      baseUrl: this.baseUrl,
      storage,
      ...(options.credentials ? { credentials: options.credentials } : {}),
      ...(options.onSessionExpired ? { onSessionExpired: options.onSessionExpired } : {}),
    });
    this.rest = new RestClient({ baseUrl: this.baseUrl, auth: this.auth });
    const wsOpts = options.ws ?? {};
    this.ws = new UltimaWS({
      url: wsOpts.url ?? this.baseUrl.replace(/^http/, "ws") + "/ws/v1",
      getToken: () => this.auth.tokens?.accessToken ?? null,
      // Refresh/login before every (re)dial so the upgrade carries a live token.
      beforeConnect: () => this.auth.getAccessToken(),
      storage,
      session: wsOpts.session,
      resume: wsOpts.resume,
      ...(wsOpts.reconnect ? { reconnect: wsOpts.reconnect } : {}),
      ...(wsOpts.onPush ? { onPush: wsOpts.onPush } : {}),
      ...(wsOpts.onGap ? { onGap: wsOpts.onGap } : {}),
      ...(wsOpts.onResumed ? { onResumed: wsOpts.onResumed } : {}),
      ...(wsOpts.onStateChange ? { onStateChange: wsOpts.onStateChange } : {}),
      ...(wsOpts.onError ? { onError: wsOpts.onError } : {}),
      ...(wsOpts.WebSocketImpl ? { WebSocketImpl: wsOpts.WebSocketImpl } : {}),
    });
  }

  /** Log in (auth-enabled servers); the next WS dial carries the token. */
  login(username: string, password: string, totp?: string): Promise<SessionInfo> {
    return this.auth.login(totp ? { username, password, totp } : { username, password });
  }

  logout(): Promise<void> {
    return this.auth.logout();
  }

  /** Open the WS connection (reconnects are automatic). */
  connect(): void {
    this.ws.connect();
  }

  close(): void {
    this.ws.close();
  }

  /** Convenience pass-throughs for the common command path. */
  ping(): Promise<string> {
    return this.ws.ping();
  }

  subscribe(channel: string, handler: PushHandler): Promise<void> {
    return this.ws.subscribe(channel, handler);
  }

  psubscribe(pattern: string, handler: PushHandler): Promise<void> {
    return this.ws.psubscribe(pattern, handler);
  }
}
