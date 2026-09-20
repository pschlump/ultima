// Admin: account table (create/edit/delete, revoke-sessions), own password
// change, and the TOTP lifecycle — enable/regenerate render the enrollment
// material (qr_code_png_base64 as an <img>, secret, provisioning URI) and
// ask for a confirmation code; disable takes password + current code.
// On auth-disabled servers the screen shows a notice instead of broken calls.
import { Fragment, useCallback, useEffect, useState, type FormEvent } from "react";
import { api, ApiError, type AccountClass, type AccountView, type TOTPEnrollment } from "../lib/api";
import { useAuth } from "../lib/auth";

function Banner({ kind, text }: { kind: "error" | "info"; text: string }) {
  return <div className={kind === "error" ? "banner banner-error" : "banner banner-info"}>{text}</div>;
}

function UserForm({
  initial,
  onSubmit,
  onCancel,
  busy,
}: {
  initial?: AccountView;
  onSubmit: (username: string, password: string, cls: AccountClass, disabled: boolean) => void;
  onCancel: () => void;
  busy: boolean;
}) {
  const [username, setUsername] = useState(initial?.username ?? "");
  const [password, setPassword] = useState("");
  const [cls, setCls] = useState<AccountClass>(initial?.class ?? "data");
  const [disabled, setDisabled] = useState(initial?.disabled ?? false);
  const submit = (e: FormEvent) => {
    e.preventDefault();
    onSubmit(username, password, cls, disabled);
  };
  return (
    <form className="inline-form" onSubmit={submit}>
      {!initial && (
        <input
          placeholder="username"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          required
        />
      )}
      {!initial && (
        <input
          type="password"
          placeholder="password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          required
        />
      )}
      <select value={cls} onChange={(e) => setCls(e.target.value === "admin" ? "admin" : "data")}>
        <option value="data">data</option>
        <option value="admin">admin</option>
      </select>
      <label className="inline-check">
        <input type="checkbox" checked={disabled} onChange={(e) => setDisabled(e.target.checked)} />
        disabled
      </label>
      <button type="submit" disabled={busy}>
        {initial ? "Save" : "Create"}
      </button>
      <button type="button" onClick={onCancel}>
        Cancel
      </button>
    </form>
  );
}

function TotpEnrollmentView({
  enroll,
  onConfirm,
  onCancel,
  busy,
}: {
  enroll: TOTPEnrollment;
  onConfirm: (code: string) => void;
  onCancel: () => void;
  busy: boolean;
}) {
  const [code, setCode] = useState("");
  return (
    <div className="card totp-card">
      <h4>Scan with your authenticator, then confirm with a code</h4>
      <img
        className="totp-qr"
        src={`data:image/png;base64,${enroll.qr_code_png_base64}`}
        alt="TOTP provisioning QR code"
      />
      <div className="kv-grid">
        <div className="kv">
          <span className="muted">secret</span>
          <span className="mono">{enroll.secret}</span>
        </div>
        <div className="kv">
          <span className="muted">provisioning URI</span>
          <span className="mono wrap">{enroll.provisioning_uri}</span>
        </div>
      </div>
      <form
        className="inline-form"
        onSubmit={(e) => {
          e.preventDefault();
          onConfirm(code);
        }}
      >
        <input
          placeholder="6-digit code"
          inputMode="numeric"
          value={code}
          onChange={(e) => setCode(e.target.value)}
          required
        />
        <button type="submit" disabled={busy}>
          Confirm
        </button>
        <button type="button" onClick={onCancel}>
          Cancel
        </button>
      </form>
    </div>
  );
}

export function Admin() {
  const { session } = useAuth();
  const [users, setUsers] = useState<AccountView[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [creating, setCreating] = useState(false);
  const [editing, setEditing] = useState<string | null>(null);
  // Own-account panels
  const [curPw, setCurPw] = useState("");
  const [newPw, setNewPw] = useState("");
  const [pwTotp, setPwTotp] = useState("");
  const [enroll, setEnroll] = useState<TOTPEnrollment | null>(null);
  const [disPw, setDisPw] = useState("");
  const [disTotp, setDisTotp] = useState("");

  const authDisabled = session?.authDisabled ?? false;
  const isAdmin = session?.accountClass === "admin";

  const load = useCallback(async () => {
    try {
      const res = await api.usersList();
      setUsers(res.users);
      setError(null);
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    }
  }, []);

  useEffect(() => {
    if (authDisabled) return;
    void load();
  }, [load, authDisabled]);

  const run = async (fn: () => Promise<unknown>, ok: string) => {
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      await fn();
      setMessage(ok);
      await load();
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  if (authDisabled) {
    return (
      <div className="screen">
        <h2>Admin</h2>
        <Banner kind="info" text="This server runs with auth.enabled=false — there are no accounts to manage. The admin API is not active." />
      </div>
    );
  }

  return (
    <div className="screen">
      <h2>Admin</h2>
      {message && <Banner kind="info" text={message} />}
      {error && <Banner kind="error" text={error} />}

      {isAdmin ? (
        <>
          <div className="toolbar">
            <h3 style={{ margin: 0 }}>Accounts</h3>
            <span className="spacer" />
            <button onClick={() => setCreating((c) => !c)}>{creating ? "Close" : "New account"}</button>
          </div>
          {creating && (
            <UserForm
              busy={busy}
              onCancel={() => setCreating(false)}
              onSubmit={(username, password, cls) =>
                void run(async () => {
                  await api.usersCreate(username, password, cls);
                  setCreating(false);
                }, `Created ${username}`)
              }
            />
          )}
          <table>
            <thead>
              <tr>
                <th>username</th>
                <th>class</th>
                <th>TOTP</th>
                <th>disabled</th>
                <th>created</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {users.map((u) => (
                <Fragment key={u.username}>
                  <tr>
                    <td className="mono">{u.username}</td>
                    <td>{u.class}</td>
                    <td>{u.totp_enabled ? "on" : "off"}</td>
                    <td>{u.disabled ? "yes" : "no"}</td>
                    <td className="mono">{new Date(u.created_at).toLocaleDateString()}</td>
                    <td className="row-actions">
                      <button onClick={() => setEditing(editing === u.username ? null : u.username)}>Edit</button>
                      <button
                        onClick={() =>
                          void run(() => api.usersRevokeSessions(u.username), `Revoked sessions for ${u.username}`)
                        }
                      >
                        Revoke sessions
                      </button>
                      <button
                        className="danger"
                        onClick={() => {
                          if (confirm(`Delete account "${u.username}"?`))
                            void run(() => api.usersDelete(u.username), `Deleted ${u.username}`);
                        }}
                      >
                        Delete
                      </button>
                    </td>
                  </tr>
                  {editing === u.username && (
                    <tr>
                      <td colSpan={6}>
                        <UserForm
                          initial={u}
                          busy={busy}
                          onCancel={() => setEditing(null)}
                          onSubmit={(_username, _password, cls, disabled) =>
                            void run(async () => {
                              await api.usersUpdate(u.username, { class: cls, disabled });
                              setEditing(null);
                            }, `Updated ${u.username}`)
                          }
                        />
                      </td>
                    </tr>
                  )}
                </Fragment>
              ))}
              {users.length === 0 && (
                <tr>
                  <td colSpan={6} className="muted">
                    No accounts.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </>
      ) : (
        <Banner kind="info" text="Account administration requires an admin account." />
      )}

      <h3>Your account ({session?.username || "?"})</h3>
      <div className="card">
        <h4>Change password</h4>
        <form
          className="inline-form"
          onSubmit={(e) => {
            e.preventDefault();
            void run(async () => {
              await api.changePassword(curPw, newPw, pwTotp || undefined);
              setCurPw("");
              setNewPw("");
              setPwTotp("");
            }, "Password changed — existing sessions were revoked; you may be asked to sign in again.");
          }}
        >
          <input
            type="password"
            placeholder="current password"
            value={curPw}
            onChange={(e) => setCurPw(e.target.value)}
            required
          />
          <input
            type="password"
            placeholder="new password"
            value={newPw}
            onChange={(e) => setNewPw(e.target.value)}
            required
          />
          <input placeholder="TOTP (if enabled)" value={pwTotp} onChange={(e) => setPwTotp(e.target.value)} />
          <button type="submit" disabled={busy}>
            Change
          </button>
        </form>
      </div>

      <div className="card">
        <h4>Two-factor authentication (TOTP)</h4>
        {enroll ? (
          <TotpEnrollmentView
            enroll={enroll}
            busy={busy}
            onCancel={() => setEnroll(null)}
            onConfirm={(code) =>
              void run(async () => {
                await api.totpConfirm(code);
                setEnroll(null);
              }, "TOTP enabled")
            }
          />
        ) : (
          <div className="toolbar">
            <button onClick={() => void run(async () => setEnroll(await api.totpEnable()), "TOTP secret staged — confirm below")} disabled={busy}>
              Enable TOTP
            </button>
            <button onClick={() => void run(async () => setEnroll(await api.totpRegenerate()), "TOTP secret regenerated — confirm below")} disabled={busy}>
              Regenerate secret
            </button>
          </div>
        )}
        <form
          className="inline-form"
          onSubmit={(e) => {
            e.preventDefault();
            if (!confirm("Disable TOTP on your account?")) return;
            void run(async () => {
              await api.totpDisable(disPw, disTotp || undefined);
              setDisPw("");
              setDisTotp("");
            }, "TOTP disabled");
          }}
        >
          <input
            type="password"
            placeholder="password"
            value={disPw}
            onChange={(e) => setDisPw(e.target.value)}
            required
          />
          <input placeholder="current TOTP code" value={disTotp} onChange={(e) => setDisTotp(e.target.value)} />
          <button type="submit" className="danger" disabled={busy}>
            Disable TOTP
          </button>
        </form>
      </div>
    </div>
  );
}
