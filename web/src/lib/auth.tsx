// Auth context (design doc §9.3, §10.2). Boot probe: GET /api/v1/info with
// any stored token — 200 enters the app (a 200 without a token means the
// server runs with auth.enabled=false, pre-M6 posture); 401 tries one
// refresh, else the login screen. The account class is read from the JWT
// access-token payload (`class` and `sub` claims, lib/auth/tokens.go) —
// decoded, not verified (the server verifies).
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { api, getTokens, onSessionExpired, refreshSession, setTokens, type AccountClass } from "./api";

export interface Session {
  username: string;
  accountClass: AccountClass;
  /** True when the server has auth.enabled=false (no token needed). */
  authDisabled: boolean;
}

interface AuthContextValue {
  /** null while the boot probe is running. */
  ready: boolean;
  session: Session | null;
  login: (username: string, password: string, totp?: string) => Promise<void>;
  logout: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | null>(null);

/** Decode the JWT payload segment without verifying (server-side verifies). */
function decodeJwtClaims(token: string): { sub: string; class: AccountClass } | null {
  const parts = token.split(".");
  const payload = parts[1];
  if (!payload) return null;
  try {
    const b64 = payload.replace(/-/g, "+").replace(/_/g, "/");
    const json = JSON.parse(atob(b64)) as { sub?: unknown; class?: unknown };
    if (typeof json.sub !== "string" || json.sub === "") return null;
    const cls: AccountClass = json.class === "admin" ? "admin" : "data";
    return { sub: json.sub, class: cls };
  } catch {
    return null;
  }
}

function sessionFromToken(accessToken: string): Session | null {
  const claims = decodeJwtClaims(accessToken);
  if (!claims) return null;
  return { username: claims.sub, accountClass: claims.class, authDisabled: false };
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [ready, setReady] = useState(false);
  const [session, setSession] = useState<Session | null>(null);

  const dropSession = useCallback(() => {
    setTokens(null);
    setSession(null);
  }, []);

  useEffect(() => {
    onSessionExpired(dropSession);
  }, [dropSession]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      const stored = getTokens();
      // Probe /info with whatever we have. On an auth-disabled server this
      // returns 200 with no token at all.
      const probe = async (): Promise<boolean> => {
        try {
          await api.info();
          return true;
        } catch {
          return false;
        }
      };
      if (await probe()) {
        if (cancelled) return;
        if (stored) {
          setSession(sessionFromToken(stored.accessToken) ?? { username: "", accountClass: "data", authDisabled: true });
        } else {
          setSession({ username: "", accountClass: "admin", authDisabled: true });
        }
        setReady(true);
        return;
      }
      // 401: try the refresh token once before showing the login screen.
      if (stored && (await refreshSession())) {
        const t = getTokens();
        if (!cancelled && t) {
          setSession(sessionFromToken(t.accessToken));
          setReady(true);
          return;
        }
      }
      if (!cancelled) {
        setTokens(null);
        setSession(null);
        setReady(true);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const login = useCallback(async (username: string, password: string, totp?: string) => {
    const pair = await api.login(totp ? { username, password, totp } : { username, password });
    setTokens({
      accessToken: pair.access_token,
      refreshToken: pair.refresh_token,
      expiresIn: pair.expires_in,
    });
    const sess = sessionFromToken(pair.access_token);
    if (!sess) throw new Error("server returned an unreadable access token");
    setSession(sess);
  }, []);

  const logout = useCallback(async () => {
    const t = getTokens();
    if (t) {
      try {
        await api.logout(t.refreshToken);
      } catch {
        // Best effort: a dead server must not trap the user on the screen.
      }
    }
    setTokens(null);
    setSession(null);
  }, []);

  const value = useMemo(
    () => ({ ready, session, login, logout }),
    [ready, session, login, logout],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth outside AuthProvider");
  return ctx;
}
