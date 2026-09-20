// Slowlog: /slowlog (with reset) and /latency tables, refresh button.
import { useCallback, useEffect, useState } from "react";
import { api, ApiError, type CommandLatency, type SlowlogEntry } from "../lib/api";

function fmtUs(us: number): string {
  if (us >= 1_000_000) return `${(us / 1_000_000).toFixed(2)} s`;
  if (us >= 1000) return `${(us / 1000).toFixed(2)} ms`;
  return `${us} µs`;
}

export function Slowlog() {
  const [entries, setEntries] = useState<SlowlogEntry[]>([]);
  const [latency, setLatency] = useState<CommandLatency[]>([]);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setError(null);
    try {
      const [sl, lat] = await Promise.all([api.slowlog(), api.latency()]);
      setEntries(sl.entries);
      setLatency(lat.commands);
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const reset = async () => {
    if (!confirm("Clear the slowlog?")) return;
    try {
      await api.resetSlowlog();
      await refresh();
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err));
    }
  };

  return (
    <div className="screen">
      <h2>Slowlog</h2>
      <div className="toolbar">
        <button onClick={() => void refresh()}>Refresh</button>
        <button className="danger" onClick={() => void reset()}>
          Reset slowlog
        </button>
      </div>
      {error && <div className="banner banner-error">{error}</div>}
      <h3>Slow log ({entries.length})</h3>
      <table>
        <thead>
          <tr>
            <th>id</th>
            <th>time</th>
            <th>duration</th>
            <th>command</th>
            <th>client</th>
          </tr>
        </thead>
        <tbody>
          {entries.map((e) => (
            <tr key={e.id}>
              <td className="mono">{e.id}</td>
              <td className="mono">{new Date(e.timestamp * 1000).toLocaleTimeString()}</td>
              <td className="mono">{fmtUs(e.duration_us)}</td>
              <td className="mono">{e.args.join(" ")}</td>
              <td className="mono">
                {e.client_addr}
                {e.client_name ? ` (${e.client_name})` : ""}
              </td>
            </tr>
          ))}
          {entries.length === 0 && (
            <tr>
              <td colSpan={5} className="muted">
                No slowlog entries.
              </td>
            </tr>
          )}
        </tbody>
      </table>

      <h3>Latency by command ({latency.length})</h3>
      <table>
        <thead>
          <tr>
            <th>command</th>
            <th>count</th>
            <th>avg</th>
            <th>max</th>
            <th>total</th>
          </tr>
        </thead>
        <tbody>
          {[...latency]
            .sort((a, b) => b.total_us - a.total_us)
            .map((c) => (
              <tr key={c.command}>
                <td className="mono">{c.command}</td>
                <td className="mono">{c.count}</td>
                <td className="mono">{fmtUs(c.avg_us)}</td>
                <td className="mono">{fmtUs(c.max_us)}</td>
                <td className="mono">{fmtUs(c.total_us)}</td>
              </tr>
            ))}
          {latency.length === 0 && (
            <tr>
              <td colSpan={5} className="muted">
                No latency stats.
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
