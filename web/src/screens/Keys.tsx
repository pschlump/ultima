// Keys: /keys/scan cursor pager with match/count inputs; click a key for a
// typed KeyPreview (per-type value rendering + TTL); delete button.
import { useCallback, useEffect, useState } from "react";
import { api, ApiError, type KeyPreview, type ZSetMember } from "../lib/client";

function fmtTtl(ms: number): string {
  if (ms < 0) return "no TTL";
  if (ms < 1000) return `${ms} ms`;
  return `${(ms / 1000).toFixed(1)} s`;
}

function PreviewValue({ preview }: { preview: KeyPreview }) {
  const v = preview.value;
  switch (preview.type) {
    case "string":
      return <pre className="key-string">{typeof v === "string" ? v : ""}</pre>;
    case "hash": {
      const obj = (v ?? {}) as Record<string, string>;
      const entries = Object.entries(obj);
      if (entries.length === 0) return <span className="muted">(empty hash)</span>;
      return (
        <table>
          <tbody>
            {entries.map(([f, val]) => (
              <tr key={f}>
                <td className="mono">{f}</td>
                <td className="mono">{val}</td>
              </tr>
            ))}
          </tbody>
        </table>
      );
    }
    case "list":
    case "set": {
      const arr = Array.isArray(v) ? (v as string[]) : [];
      if (arr.length === 0) return <span className="muted">(empty)</span>;
      return (
        <ol>
          {arr.map((e, i) => (
            <li key={i} className="mono">
              {e}
            </li>
          ))}
        </ol>
      );
    }
    case "zset": {
      const arr = Array.isArray(v) ? (v as ZSetMember[]) : [];
      if (arr.length === 0) return <span className="muted">(empty zset)</span>;
      return (
        <table>
          <tbody>
            {arr.map((m, i) => (
              <tr key={i}>
                <td className="mono">{m.member}</td>
                <td className="mono">{m.score}</td>
              </tr>
            ))}
          </tbody>
        </table>
      );
    }
  }
}

export function Keys() {
  const [match, setMatch] = useState("");
  const [count, setCount] = useState(100);
  const [keys, setKeys] = useState<string[]>([]);
  // Cursor stack for back/forward pagination ("0" restarts the scan).
  const [cursor, setCursor] = useState("0");
  const [cursorStack, setCursorStack] = useState<string[]>([]);
  const [nextCursor, setNextCursor] = useState("0");
  const [selected, setSelected] = useState<KeyPreview | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const scan = useCallback(
    async (cur: string, m: string, c: number) => {
      setLoading(true);
      setError(null);
      try {
        const res = await api.scanKeys(cur, m, c);
        setKeys(res.keys);
        setNextCursor(res.cursor);
        setCursor(cur);
        setSelected(null);
      } catch (err) {
        setError(err instanceof ApiError ? err.message : String(err));
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  useEffect(() => {
    void scan("0", "", 100);
  }, [scan]);

  const applyFilter = () => {
    setCursorStack([]);
    void scan("0", match, count);
  };

  const next = () => {
    setCursorStack((s) => [...s, cursor]);
    void scan(nextCursor, match, count);
  };

  const back = () => {
    const prev = cursorStack[cursorStack.length - 1];
    if (prev === undefined) return;
    setCursorStack((s) => s.slice(0, -1));
    void scan(prev, match, count);
  };

  const openKey = async (key: string) => {
    setError(null);
    try {
      setSelected(await api.getKey(key));
    } catch (err) {
      setSelected(null);
      setError(err instanceof ApiError ? err.message : String(err));
    }
  };

  const delKey = async () => {
    if (!selected) return;
    if (!confirm(`Delete key "${selected.key}"?`)) return;
    try {
      await api.deleteKey(selected.key);
      setKeys((ks) => ks.filter((k) => k !== selected.key));
      setSelected(null);
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    }
  };

  return (
    <div className="screen">
      <h2>Keys</h2>
      <div className="toolbar">
        <input
          placeholder="MATCH pattern (glob)"
          value={match}
          onChange={(e) => setMatch(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && applyFilter()}
        />
        <input
          type="number"
          min={1}
          style={{ width: "6rem" }}
          title="COUNT hint"
          value={count}
          onChange={(e) => setCount(Number(e.target.value) || 100)}
        />
        <button onClick={applyFilter} disabled={loading}>
          Scan
        </button>
        <button onClick={back} disabled={loading || cursorStack.length === 0}>
          ← Prev
        </button>
        <button onClick={next} disabled={loading || nextCursor === "0"}>
          Next →
        </button>
        <span className="muted mono">cursor {cursor} → {nextCursor}</span>
      </div>
      {error && <div className="banner banner-error">{error}</div>}
      <div className="keys-layout">
        <div className="key-list">
          {keys.length === 0 && !loading && <p className="muted">No keys in this batch.</p>}
          {keys.map((k) => (
            <button
              key={k}
              className={selected?.key === k ? "key-item key-item-active" : "key-item"}
              onClick={() => void openKey(k)}
            >
              {k}
            </button>
          ))}
        </div>
        <div className="key-detail">
          {selected ? (
            <>
              <div className="toolbar">
                <span className="tag">{selected.type}</span>
                <span className="mono">{selected.key}</span>
                <span className="muted">TTL {fmtTtl(selected.ttl_ms)}</span>
                <span className="spacer" />
                <button className="danger" onClick={() => void delKey()}>
                  Delete
                </button>
              </div>
              <PreviewValue preview={selected} />
            </>
          ) : (
            <p className="muted">Select a key to preview it.</p>
          )}
        </div>
      </div>
    </div>
  );
}
