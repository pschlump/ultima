// Config: GET /config editable table, PUT only the dirty rows; action
// buttons for save/bgsave/bgrewriteaof/flushdb (with confirm).
import { useCallback, useEffect, useState } from "react";
import { api, ApiError } from "../lib/api";

export function Config() {
  const [original, setOriginal] = useState<Record<string, string>>({});
  const [values, setValues] = useState<Record<string, string>>({});
  const [filter, setFilter] = useState("");
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const load = useCallback(async () => {
    setError(null);
    try {
      const res = await api.getConfig();
      setOriginal(res.entries);
      setValues(res.entries);
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const dirty = Object.entries(values).filter(([k, v]) => original[k] !== v);

  const apply = async () => {
    if (dirty.length === 0) return;
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      await api.putConfig(Object.fromEntries(dirty));
      setMessage(`Applied ${dirty.length} parameter${dirty.length === 1 ? "" : "s"}.`);
      await load();
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  const action = async (label: string, fn: () => Promise<unknown>, confirmText?: string) => {
    if (confirmText && !confirm(confirmText)) return;
    setBusy(true);
    setError(null);
    setMessage(null);
    try {
      await fn();
      setMessage(`${label}: ok`);
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  };

  const names = Object.keys(values)
    .filter((k) => filter === "" || k.includes(filter.toLowerCase()))
    .sort();

  return (
    <div className="screen">
      <h2>Config</h2>
      <div className="toolbar">
        <input
          placeholder="filter parameters"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
        />
        <button onClick={() => void load()} disabled={busy}>
          Reload
        </button>
        <button onClick={() => void apply()} disabled={busy || dirty.length === 0}>
          Apply {dirty.length > 0 ? `(${dirty.length} dirty)` : ""}
        </button>
        <span className="spacer" />
        <button onClick={() => void action("SAVE", api.save, "SAVE blocks the server while writing a snapshot. Continue?")} disabled={busy}>
          SAVE
        </button>
        <button onClick={() => void action("BGSAVE", api.bgsave)} disabled={busy}>
          BGSAVE
        </button>
        <button onClick={() => void action("BGREWRITEAOF", api.bgrewriteaof)} disabled={busy}>
          BGREWRITEAOF
        </button>
        <button
          className="danger"
          onClick={() => void action("FLUSHDB", () => api.flushdb(0, false), "Delete every key in DB 0? This cannot be undone.")}
          disabled={busy}
        >
          FLUSHDB
        </button>
      </div>
      {message && <div className="banner banner-info">{message}</div>}
      {error && <div className="banner banner-error">{error}</div>}
      <table>
        <thead>
          <tr>
            <th>parameter</th>
            <th>value</th>
          </tr>
        </thead>
        <tbody>
          {names.map((name) => {
            const isDirty = original[name] !== values[name];
            return (
              <tr key={name} className={isDirty ? "row-dirty" : ""}>
                <td className="mono">{name}</td>
                <td>
                  <input
                    className="cell-input"
                    value={values[name] ?? ""}
                    onChange={(e) => setValues((v) => ({ ...v, [name]: e.target.value }))}
                  />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
