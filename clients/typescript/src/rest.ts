// Typed client for the Ultima management REST API (design doc §10.1,
// contract in api/openapi.yaml). Every request can carry a Bearer access
// token from a TokenProvider (the AuthManager); on 401 the wrapper runs one
// refresh and retries the request once. Error replies keep the server's
// {"status":"error","error":msg} message.

/** Supplies bearer tokens; implemented by AuthManager. */
export interface TokenProvider {
  /** Current usable access token (refreshing proactively), or null. */
  getAccessToken(): Promise<string | null>;
  /** Single-flight refresh after a 401; false = session unrecoverable. */
  refresh(): Promise<boolean>;
}

export class ApiError extends Error {
  readonly status: number;
  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

export interface RestClientOptions {
  /** e.g. "http://127.0.0.1:6381" (no trailing slash needed). */
  baseUrl: string;
  auth?: TokenProvider;
  /** Injectable fetch (defaults to the global). */
  fetchImpl?: typeof fetch;
}

// ---------------------------------------------------------------------------
// Response types (mirror api/openapi.yaml schemas)
// ---------------------------------------------------------------------------

export interface Status {
  status: string;
}

export interface TokenPair {
  access_token: string;
  refresh_token: string;
  expires_in: number;
}

export interface LoginRequest {
  username: string;
  password: string;
  totp?: string;
}

export type AccountClass = "admin" | "data";

export interface AccountView {
  username: string;
  class: AccountClass;
  totp_enabled: boolean;
  disabled: boolean;
  created_at: string;
  updated_at: string;
}

export interface TOTPEnrollment {
  secret: string;
  provisioning_uri: string;
  qr_code_png_base64: string;
}

/** sections → fields; values mirror the RESP INFO strings. */
export interface Info {
  sections: Record<string, Record<string, string>>;
}

export interface ShardStat {
  index: number;
  keys: number;
  expires: number;
  mem_bytes: number;
  queue_depth: number;
  expiry_heap: number;
}

export interface ClientInfo {
  id: number;
  addr: string;
  name: string;
  user: string;
  db: number;
  proto: number;
  age_seconds: number;
  idle_seconds: number;
  last_command: string;
  subscriptions: number;
  patterns: number;
  surface: "resp" | "grpc" | "ws";
}

export interface SlowlogEntry {
  id: number;
  timestamp: number;
  duration_us: number;
  args: string[];
  client_addr: string;
  client_name: string;
}

export interface CommandLatency {
  command: string;
  count: number;
  total_us: number;
  max_us: number;
  avg_us: number;
}

export interface ScanResult {
  /** Next cursor; "0" means the scan is done. */
  cursor: string;
  keys: string[];
}

export interface ZSetMember {
  member: string;
  score: string;
}

export type KeyPreviewValue = string | string[] | Record<string, string> | ZSetMember[] | null;

export interface KeyPreview {
  key: string;
  type: "string" | "hash" | "list" | "set" | "zset";
  /** -1 = no TTL. */
  ttl_ms: number;
  value: KeyPreviewValue;
}

export interface KeyDeleted {
  deleted: boolean;
}

type Json = Record<string, unknown> | undefined;

export class RestClient {
  private readonly baseUrl: string;
  private readonly auth?: TokenProvider;
  private readonly fetchImpl: typeof fetch;

  constructor(options: RestClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.auth = options.auth;
    this.fetchImpl = options.fetchImpl ?? fetch;
  }

  private async rawRequest(path: string, init: RequestInit): Promise<Response> {
    const headers = new Headers(init.headers);
    if (init.body !== undefined && !headers.has("Content-Type")) {
      headers.set("Content-Type", "application/json");
    }
    const token = this.auth ? await this.auth.getAccessToken() : null;
    if (token) headers.set("Authorization", `Bearer ${token}`);
    return this.fetchImpl(this.baseUrl + path, { ...init, headers });
  }

  private async request<T>(method: string, path: string, body?: Json): Promise<T> {
    const init: RequestInit = { method };
    if (body !== undefined) init.body = JSON.stringify(body);
    let res = await this.rawRequest(path, init);
    if (res.status === 401 && this.auth) {
      if (await this.auth.refresh()) {
        res = await this.rawRequest(path, init);
      }
    }
    if (!res.ok) throw await errorFrom(res);
    if (res.status === 204) return undefined as T;
    return (await res.json()) as T;
  }

  // --- auth (public endpoints; also used by AuthManager) ---

  ping(): Promise<{ message: string }> {
    return this.request("GET", "/api/v1/ping");
  }

  login(body: LoginRequest): Promise<TokenPair> {
    return this.request("POST", "/api/v1/auth/login", body as unknown as Json);
  }

  refreshToken(refreshToken: string): Promise<TokenPair> {
    return this.request("POST", "/api/v1/auth/refresh", { refresh_token: refreshToken });
  }

  logout(refreshToken: string): Promise<Status> {
    return this.request("POST", "/api/v1/auth/logout", { refresh_token: refreshToken });
  }

  changePassword(currentPassword: string, newPassword: string, totp?: string): Promise<Status> {
    return this.request("POST", "/api/v1/auth/password", {
      current_password: currentPassword,
      new_password: newPassword,
      ...(totp ? { totp } : {}),
    });
  }

  totpEnable(): Promise<TOTPEnrollment> {
    return this.request("POST", "/api/v1/auth/totp/enable");
  }

  totpRegenerate(): Promise<TOTPEnrollment> {
    return this.request("POST", "/api/v1/auth/totp/regenerate");
  }

  totpConfirm(code: string): Promise<Status> {
    return this.request("POST", "/api/v1/auth/totp/confirm", { totp: code });
  }

  totpDisable(password: string, totp?: string): Promise<Status> {
    return this.request("POST", "/api/v1/auth/totp/disable", { password, ...(totp ? { totp } : {}) });
  }

  // --- admin ---

  usersList(): Promise<{ users: AccountView[] }> {
    return this.request("GET", "/api/v1/admin/users");
  }

  usersCreate(username: string, password: string, cls: AccountClass): Promise<AccountView> {
    return this.request("POST", "/api/v1/admin/users", { username, password, class: cls });
  }

  usersUpdate(name: string, update: { class?: AccountClass; disabled?: boolean }): Promise<AccountView> {
    return this.request("PUT", `/api/v1/admin/users/${encodeURIComponent(name)}`, update as Json);
  }

  usersDelete(name: string): Promise<Status> {
    return this.request("DELETE", `/api/v1/admin/users/${encodeURIComponent(name)}`);
  }

  usersRevokeSessions(name: string): Promise<Status> {
    return this.request("POST", `/api/v1/admin/users/${encodeURIComponent(name)}/revoke-sessions`);
  }

  // --- server ---

  info(): Promise<Info> {
    return this.request("GET", "/api/v1/info");
  }

  shards(): Promise<{ shards: ShardStat[] }> {
    return this.request("GET", "/api/v1/shards");
  }

  clients(): Promise<{ clients: ClientInfo[] }> {
    return this.request("GET", "/api/v1/clients");
  }

  killClient(id: number): Promise<Status> {
    return this.request("POST", `/api/v1/clients/${id}/kill`);
  }

  slowlog(count?: number): Promise<{ entries: SlowlogEntry[] }> {
    return this.request("GET", `/api/v1/slowlog${count ? `?count=${count}` : ""}`);
  }

  resetSlowlog(): Promise<Status> {
    return this.request("DELETE", "/api/v1/slowlog");
  }

  latency(): Promise<{ commands: CommandLatency[] }> {
    return this.request("GET", "/api/v1/latency");
  }

  getConfig(): Promise<{ entries: Record<string, string> }> {
    return this.request("GET", "/api/v1/config");
  }

  putConfig(entries: Record<string, string>): Promise<Status> {
    return this.request("PUT", "/api/v1/config", { entries });
  }

  // --- data ---

  flushdb(db: number, all: boolean): Promise<Status> {
    return this.request("POST", "/api/v1/flushdb", { db, all });
  }

  scanKeys(cursor: string, match = "", count = 0, db = 0): Promise<ScanResult> {
    const q = new URLSearchParams({ cursor, db: String(db) });
    if (match) q.set("match", match);
    if (count > 0) q.set("count", String(count));
    return this.request("GET", `/api/v1/keys/scan?${q.toString()}`);
  }

  getKey(key: string, db = 0): Promise<KeyPreview> {
    return this.request("GET", `/api/v1/key/${encodeURIComponent(key)}?db=${db}`);
  }

  deleteKey(key: string, db = 0): Promise<KeyDeleted> {
    return this.request("DELETE", `/api/v1/key/${encodeURIComponent(key)}?db=${db}`);
  }

  // --- persistence ---

  save(): Promise<Status> {
    return this.request("POST", "/api/v1/save");
  }

  bgsave(): Promise<Status> {
    return this.request("POST", "/api/v1/bgsave");
  }

  bgrewriteaof(): Promise<Status> {
    return this.request("POST", "/api/v1/bgrewriteaof");
  }
}

async function errorFrom(res: Response): Promise<ApiError> {
  let msg = `${res.status} ${res.statusText}`;
  try {
    const body = (await res.json()) as { status?: string; error?: string };
    if (typeof body.error === "string" && body.error !== "") msg = body.error;
  } catch {
    // non-JSON error body; keep the HTTP status line
  }
  return new ApiError(res.status, msg);
}
