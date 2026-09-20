// The web UI is @ultima/client's first consumer (design doc §11.2): this
// module wires the library's umbrella client to the same-origin deployment
// (the UI is served by ultima-server itself; the vite dev server proxies
// /api and /ws) and re-exports the pieces the screens use. The REST wrapper,
// token store/refresh, and WS client implementations live in
// clients/typescript — not here.
import { ApiError, UltimaClient, UltimaWS, type UltimaWSOptions } from "@ultima/client";

// Invoked when the auth session is unrecoverable (refresh-token rejected);
// the auth context registers its drop-session callback at mount.
let expiredHandler: (() => void) | null = null;
export function onSessionExpired(fn: () => void): void {
  expiredHandler = fn;
}

export const client = new UltimaClient({
  baseUrl: window.location.origin,
  onSessionExpired: () => expiredHandler?.(),
});

/** Typed REST management API (Bearer + single-flight refresh built in). */
export const api = client.rest;

/** Token lifecycle: login/refresh/re-login, auth-disabled probe. */
export const auth = client.auth;

/** Same-origin /ws/v1 endpoint (proxied by vite in dev). */
export function wsURL(): string {
  const proto = window.location.protocol === "https:" ? "wss" : "ws";
  return `${proto}://${window.location.host}/ws/v1`;
}

/** Access token for the WS upgrade, or null when logged out. */
export function getAccessToken(): string | null {
  return auth.tokens?.accessToken ?? null;
}

/** One WS connection per screen (Console/Monitor/PubSub): library client
 * with the §9.4 session protocol, fed with the current access token; the
 * token is refreshed before every (re)dial. */
export function newUltimaWS(options: Omit<UltimaWSOptions, "url" | "getToken" | "beforeConnect">): UltimaWS {
  return new UltimaWS({
    ...options,
    url: wsURL(),
    getToken: getAccessToken,
    beforeConnect: () => auth.getAccessToken(),
  });
}

export { ApiError, UltimaWS };
export type {
  AccountClass,
  AccountView,
  CommandLatency,
  Info,
  KeyPreview,
  ShardStat,
  SlowlogEntry,
  TOTPEnrollment,
  WSState,
  ZSetMember,
} from "@ultima/client";
