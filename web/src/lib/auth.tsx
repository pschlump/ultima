// Auth context (design doc §9.3, §10.2) — a thin React wrapper over
// @ultima/client's AuthManager (lib/client.ts): the boot probe asks whether
// the server requires auth (GET /api/v1/info without a token — 200 means
// auth.enabled=false, the pre-M6 posture), a stored token that still
// verifies (or refreshes) enters the app, otherwise the login screen.
// The account class is read from the JWT access-token payload (`class` and
// `sub` claims, lib/auth/tokens.go) — decoded, not verified (the server
// verifies).
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { auth, onSessionExpired, type AccountClass } from "./client";

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

export function AuthProvider({ children }: { children: ReactNode }) {
  const [ready, setReady] = useState(false);
  const [session, setSession] = useState<Session | null>(null);

  const dropSession = useCallback(() => {
    setSession(null);
  }, []);

  useEffect(() => {
    onSessionExpired(dropSession);
  }, [dropSession]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      const { authRequired } = await auth.probe();
      if (cancelled) return;
      if (!authRequired) {
        // Pre-M6 posture: no token needed. A decodable stored token still
        // supplies the user chip, as before.
        const decoded = auth.session();
        setSession(
          decoded ?? {
            username: "",
            accountClass: auth.tokens ? "data" : "admin",
            authDisabled: true,
          },
        );
        setReady(true);
        return;
      }
      // A usable stored token (proactively refreshed when near expiry, or
      // rotated once via the refresh token) enters the app; otherwise the
      // login screen. A failed refresh drops the tokens inside AuthManager
      // and fires onSessionExpired.
      const token = await auth.getAccessToken();
      if (cancelled) return;
      setSession(token ? auth.session() : null);
      setReady(true);
    })().catch(() => {
      if (!cancelled) {
        setSession(null);
        setReady(true);
      }
    });
    return () => {
      cancelled = true;
    };
  }, []);

  const login = useCallback(async (username: string, password: string, totp?: string) => {
    await auth.login(totp ? { username, password, totp } : { username, password });
    const sess = auth.session();
    if (!sess) throw new Error("server returned an unreadable access token");
    setSession(sess);
  }, []);


  const logout = useCallback(async () => {
    await auth.logout();
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
