// Typed fetch wrappers for the Ultima management REST API (design doc
// §10.1, contract in api/openapi.yaml). Every request carries the Bearer
// access token from the module-level token store; on 401 the wrapper runs
// a single-flight POST /api/v1/auth/refresh and retries the request once.
// Refresh failure logs the session out via a callback registered by the
// auth context.

// ---------------------------------------------------------------------------
// Token store
// ---------------------------------------------------------------------------

export interface TokenSet {
  accessToken: string;
  refreshToken: string;
  expiresIn: number;
}

const STORAGE_KEY = "ultima.tokens";

let tokens: TokenSet | null = null;

function loadTokens(): TokenSet | null {
  if (tokens) return tokens;
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as TokenSet;
    if (typeof parsed.accessToken !== "string" || typeof parsed.refreshToken !== "string") {
      return null;
    }
    tokens = parsed;
    return tokens;
  } catch {
    return null;
  }
}

export function getTokens(): TokenSet | null {
  return loadTokens();
}

export function setTokens(t: TokenSet | null): void {
  tokens = t;
  if (t) {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(t));
  } else {
    localStorage.removeItem(STORAGE_KEY);
  }
}

// Registered by the auth context; invoked when the session is unrecoverable.
let sessionExpiredHandler: (() => void) | null = null;
export function onSessionExpired(fn: () => void): void {
  sessionExpiredHandler = fn;
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export class ApiError extends Error {
  readonly status: number;
  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
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

// ---------------------------------------------------------------------------
// Refresh (single-flight)
// ---------------------------------------------------------------------------

let refreshInFlight: Promise<boolean> | null = null;

async function doRefresh(): Promise<boolean> {
  const t = loadTokens();
  if (!t) return false;
  try {
    const res = await fetch("/api/v1/auth/refresh", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ refresh_token: t.refreshToken }),
    });
    if (!res.ok) return false;
    const pair = (await res.json()) as TokenPair;
    setTokens({
      accessToken: pair.access_token,
      refreshToken: pair.refresh_token,
      expiresIn: pair.expires_in,
    });
    return true;
  } catch {
    return false;
  }
}

export function refreshSession(): Promise<boolean> {
  if (!refreshInFlight) {
    refreshInFlight = doRefresh().finally(() => {
      refreshInFlight = null;
    });
  }
  return refreshInFlight;
}

// ---------------------------------------------------------------------------
// Core request
// ---------------------------------------------------------------------------

type Json = Record<string, unknown> | undefined;

async function rawRequest(path: string, init: RequestInit): Promise<Response> {
  const t = loadTokens();
  const headers = new Headers(init.headers);
  if (init.body !== undefined && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  if (t) headers.set("Authorization", `Bearer ${t.accessToken}`);
  return fetch(path, { ...init, headers });
}

async function request<T>(method: string, path: string, body?: Json): Promise<T> {
  const init: RequestInit = { method };
  if (body !== undefined) init.body = JSON.stringify(body);
  let res = await rawRequest(path, init);
  if (res.status === 401 && loadTokens()) {
    if (await refreshSession()) {
      res = await rawRequest(path, init);
    } else {
      sessionExpiredHandler?.();
      throw new ApiError(401, "session expired");
    }
  }
  if (!res.ok) throw await errorFrom(res);
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
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
  cursor: string;
  keys: string[];
}

export interface ZSetMember {
  member: string;
  score: string;
}

export type KeyPreviewValue =
  | string
  | string[]
  | Record<string, string>
  | ZSetMember[]
  | null;

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

// ---------------------------------------------------------------------------
// Endpoint wrappers
// ---------------------------------------------------------------------------

export const api = {
  // auth
  login: (body: LoginRequest) => request<TokenPair>("POST", "/api/v1/auth/login", body as unknown as Json),
  logout: (refreshToken: string) =>
    request<Status>("POST", "/api/v1/auth/logout", { refresh_token: refreshToken }),
  changePassword: (currentPassword: string, newPassword: string, totp?: string) =>
    request<Status>("POST", "/api/v1/auth/password", {
      current_password: currentPassword,
      new_password: newPassword,
      ...(totp ? { totp } : {}),
    }),
  totpEnable: () => request<TOTPEnrollment>("POST", "/api/v1/auth/totp/enable"),
  totpRegenerate: () => request<TOTPEnrollment>("POST", "/api/v1/auth/totp/regenerate"),
  totpConfirm: (code: string) => request<Status>("POST", "/api/v1/auth/totp/confirm", { totp: code }),
  totpDisable: (password: string, totp?: string) =>
    request<Status>("POST", "/api/v1/auth/totp/disable", { password, ...(totp ? { totp } : {}) }),

  // admin
  usersList: () => request<{ users: AccountView[] }>("GET", "/api/v1/admin/users"),
  usersCreate: (username: string, password: string, cls: AccountClass) =>
    request<AccountView>("POST", "/api/v1/admin/users", { username, password, class: cls }),
  usersUpdate: (name: string, update: { class?: AccountClass; disabled?: boolean }) =>
    request<AccountView>("PUT", `/api/v1/admin/users/${encodeURIComponent(name)}`, update as Json),
  usersDelete: (name: string) =>
    request<Status>("DELETE", `/api/v1/admin/users/${encodeURIComponent(name)}`),
  usersRevokeSessions: (name: string) =>
    request<Status>("POST", `/api/v1/admin/users/${encodeURIComponent(name)}/revoke-sessions`),

  // server
  info: () => request<Info>("GET", "/api/v1/info"),
  shards: () => request<{ shards: ShardStat[] }>("GET", "/api/v1/shards"),
  clients: () => request<{ clients: ClientInfo[] }>("GET", "/api/v1/clients"),
  killClient: (id: number) => request<Status>("POST", `/api/v1/clients/${id}/kill`),
  slowlog: (count?: number) =>
    request<{ entries: SlowlogEntry[] }>("GET", `/api/v1/slowlog${count ? `?count=${count}` : ""}`),
  resetSlowlog: () => request<Status>("DELETE", "/api/v1/slowlog"),
  latency: () => request<{ commands: CommandLatency[] }>("GET", "/api/v1/latency"),
  getConfig: () => request<{ entries: Record<string, string> }>("GET", "/api/v1/config"),
  putConfig: (entries: Record<string, string>) =>
    request<Status>("PUT", "/api/v1/config", { entries }),

  // data
  flushdb: (db: number, all: boolean) => request<Status>("POST", "/api/v1/flushdb", { db, all }),
  scanKeys: (cursor: string, match: string, count: number, db = 0) => {
    const q = new URLSearchParams({ cursor, db: String(db) });
    if (match) q.set("match", match);
    if (count > 0) q.set("count", String(count));
    return request<ScanResult>("GET", `/api/v1/keys/scan?${q.toString()}`);
  },
  getKey: (key: string, db = 0) =>
    request<KeyPreview>("GET", `/api/v1/key/${encodeURIComponent(key)}?db=${db}`),
  deleteKey: (key: string, db = 0) =>
    request<KeyDeleted>("DELETE", `/api/v1/key/${encodeURIComponent(key)}?db=${db}`),

  // persistence
  save: () => request<Status>("POST", "/api/v1/save"),
  bgsave: () => request<Status>("POST", "/api/v1/bgsave"),
  bgrewriteaof: () => request<Status>("POST", "/api/v1/bgrewriteaof"),
};
