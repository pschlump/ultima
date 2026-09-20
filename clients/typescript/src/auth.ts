// AuthManager: token lifecycle for the M6a auth core (design doc §9, D20):
// password (+optional TOTP) login, proactive access-token refresh before the
// JWT exp claim, single-flight refresh after a 401, re-login with the stored
// credentials when the refresh token is dead, and an auth-enabled probe
// (GET /api/v1/info without a token: 200 means the server runs with
// auth.enabled=false, the pre-M6 posture).
import { RestClient, ApiError, type AccountClass, type TokenProvider } from "./rest";
import { defaultStorage, type StorageLike } from "./storage";

export interface Credentials {
  username: string;
  password: string;
  totp?: string;
}

export interface SessionInfo {
  username: string;
  accountClass: AccountClass;
  /** True when the server has auth.enabled=false (no token needed). */
  authDisabled: boolean;
}

export interface TokenSet {
  accessToken: string;
  refreshToken: string;
  /** Epoch milliseconds after which the access token should be refreshed. */
  expiresAt: number;
}

export interface AuthManagerOptions {
  baseUrl: string;
  /** Stored for automatic re-login when the refresh token expires. */
  credentials?: Credentials;
  /** Token persistence; defaults to localStorage or in-memory. */
  storage?: StorageLike;
  storageKey?: string;
  /** Refresh this long before expiry. Default 30_000 ms. */
  refreshMarginMs?: number;
  fetchImpl?: typeof fetch;
  /** The session is unrecoverable: no token, no working credentials. */
  onSessionExpired?: () => void;
}

interface JwtClaims {
  sub?: unknown;
  class?: unknown;
  exp?: unknown;
}

function decodeJwtClaims(token: string): JwtClaims | null {
  const payload = token.split(".")[1];
  if (!payload) return null;
  try {
    const b64 = payload.replace(/-/g, "+").replace(/_/g, "/");
    return JSON.parse(atob(b64)) as JwtClaims;
  } catch {
    return null;
  }
}

export class AuthManager implements TokenProvider {
  private readonly rest: RestClient;
  private readonly storage: StorageLike;
  private readonly storageKey: string;
  private readonly marginMs: number;
  private tokensValue: TokenSet | null;
  private refreshInFlight: Promise<boolean> | null = null;

  constructor(private readonly options: AuthManagerOptions) {
    this.rest = new RestClient({ baseUrl: options.baseUrl, ...(options.fetchImpl ? { fetchImpl: options.fetchImpl } : {}) });
    this.storage = options.storage ?? defaultStorage();
    this.storageKey = options.storageKey ?? "ultima.tokens";
    this.marginMs = options.refreshMarginMs ?? 30_000;
    this.tokensValue = this.loadStored();
  }

  /** Current tokens, or null when logged out. */
  get tokens(): TokenSet | null {
    return this.tokensValue;
  }

  /** True when the server requires authentication (auth.enabled=true). */
  async probe(): Promise<{ authRequired: boolean }> {
    try {
      await this.rest.info();
      return { authRequired: false };
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) return { authRequired: true };
      throw err;
    }
  }

  /** Session facts decoded from the current access token (not verified —
   * the server verifies), or null when logged out. */
  session(): SessionInfo | null {
    const t = this.tokensValue;
    if (!t) return null;
    const claims = decodeJwtClaims(t.accessToken);
    if (!claims || typeof claims.sub !== "string" || claims.sub === "") return null;
    return {
      username: claims.sub,
      accountClass: claims.class === "admin" ? "admin" : "data",
      authDisabled: false,
    };
  }

  /** Password login; stores the token pair on success. */
  async login(credentials?: Credentials): Promise<SessionInfo> {
    const creds = credentials ?? this.options.credentials;
    if (!creds) throw new Error("AuthManager.login: no credentials");
    const pair = await this.rest.login(
      creds.totp
        ? { username: creds.username, password: creds.password, totp: creds.totp }
        : { username: creds.username, password: creds.password },
    );
    this.setTokens(pair.access_token, pair.refresh_token, pair.expires_in);
    const sess = this.session();
    if (!sess) throw new Error("AuthManager.login: server returned an unreadable access token");
    return sess;
  }

  /** Revoke the refresh-token family and drop the tokens. */
  async logout(): Promise<void> {
    const t = this.tokensValue;
    if (t) {
      try {
        await this.rest.logout(t.refreshToken);
      } catch {
        // best effort: a dead server must not trap the caller
      }
    }
    this.setTokens(null);
  }

  /** TokenProvider: a usable access token, refreshing/re-logging in as
   * needed. Null when the server has auth disabled or no session exists. */
  async getAccessToken(): Promise<string | null> {
    const t = this.tokensValue;
    if (!t) {
      if (this.options.credentials) {
        await this.login();
        return this.tokensValue?.accessToken ?? null;
      }
      return null;
    }
    if (t.expiresAt - Date.now() > this.marginMs) return t.accessToken;
    if (await this.refresh()) return this.tokensValue?.accessToken ?? null;
    // refresh() with tokens present has already dropped them and fired
    // onSessionExpired; a credential re-login is the last resort.
    if (this.options.credentials) {
      await this.login();
      return this.tokensValue?.accessToken ?? null;
    }
    return null;
  }

  /** TokenProvider: single-flight refresh-token rotation. A failed refresh
   * with tokens present ends the session: tokens are dropped and
   * onSessionExpired fires. */
  refresh(): Promise<boolean> {
    if (!this.refreshInFlight) {
      const had = this.tokensValue !== null;
      this.refreshInFlight = this.doRefresh()
        .then((ok) => {
          if (!ok && had) {
            this.setTokens(null);
            this.options.onSessionExpired?.();
          }
          return ok;
        })
        .finally(() => {
          this.refreshInFlight = null;
        });
    }
    return this.refreshInFlight;
  }

  private async doRefresh(): Promise<boolean> {
    const t = this.tokensValue;
    if (!t) return false;
    try {
      const pair = await this.rest.refreshToken(t.refreshToken);
      this.setTokens(pair.access_token, pair.refresh_token, pair.expires_in);
      return true;
    } catch {
      return false;
    }
  }

  private setTokens(accessToken: string | null, refreshToken?: string, expiresIn?: number): void {
    if (accessToken === null) {
      this.tokensValue = null;
      this.storage.removeItem(this.storageKey);
      return;
    }
    // Prefer the JWT exp claim; fall back to expires_in.
    let expiresAt = Date.now() + (expiresIn ?? 0) * 1000;
    const claims = decodeJwtClaims(accessToken);
    if (claims && typeof claims.exp === "number" && claims.exp > 0) {
      expiresAt = claims.exp * 1000;
    }
    this.tokensValue = { accessToken, refreshToken: refreshToken ?? "", expiresAt };
    try {
      this.storage.setItem(this.storageKey, JSON.stringify(this.tokensValue));
    } catch {
      // storage full/blocked — tokens live in memory only
    }
  }

  private loadStored(): TokenSet | null {
    try {
      const raw = this.storage.getItem(this.storageKey);
      if (!raw) return null;
      const parsed = JSON.parse(raw) as Partial<TokenSet>;
      if (typeof parsed.accessToken !== "string" || typeof parsed.refreshToken !== "string") return null;
      return {
        accessToken: parsed.accessToken,
        refreshToken: parsed.refreshToken,
        expiresAt: typeof parsed.expiresAt === "number" ? parsed.expiresAt : 0,
      };
    } catch {
      return null;
    }
  }
}
