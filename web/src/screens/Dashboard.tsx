// Dashboard: polls /info + /shards every ~2s. Shows version/uptime/clients,
// ops/sec computed from deltas of total_commands_processed, used_memory vs
// maxmemory bar, expired/evicted counters, pubsub counts, and a per-shard
// heatmap grid colored by keys (click a cell for the exact numbers).
import { useEffect, useRef, useState } from "react";
import { api, type Info, type ShardStat } from "../lib/api";

function num(s: string | undefined): number {
  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

function fmtBytes(n: number): string {
  if (n <= 0) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let u = 0;
  let v = n;
  while (v >= 1024 && u < units.length - 1) {
    v /= 1024;
    u++;
  }
  return `${v.toFixed(v >= 100 ? 0 : 1)} ${units[u]}`;
}

function fmtUptime(sec: number): string {
  const d = Math.floor(sec / 86400);
  const h = Math.floor((sec % 86400) / 3600);
  const m = Math.floor((sec % 3600) / 60);
  if (d > 0) return `${d}d ${h}h ${m}m`;
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m ${Math.floor(sec % 60)}s`;
}

interface Stats {
  version: string;
  uptime: number;
  clients: number;
  blockedClients: number;
  totalCommands: number;
  usedMemory: number;
  maxmemory: number;
  maxmemoryPolicy: string;
  expiredKeys: number;
  evictedKeys: number;
  pubsubChannels: number;
  pubsubPatterns: number;
  keyspace: Record<string, string>;
}

function readStats(info: Info): Stats {
  const s = info.sections;
  const server = s["server"] ?? {};
  const clients = s["clients"] ?? {};
  const memory = s["memory"] ?? {};
  const stats = s["stats"] ?? {};
  const keyspace = s["keyspace"] ?? {};
  return {
    version: server["ultima_version"] ?? server["redis_version"] ?? "?",
    uptime: num(server["uptime_in_seconds"]),
    clients: num(clients["connected_clients"]),
    blockedClients: num(clients["blocked_clients"]),
    totalCommands: num(stats["total_commands_processed"]),
    usedMemory: num(memory["used_memory"]),
    maxmemory: num(memory["maxmemory"]),
    maxmemoryPolicy: memory["maxmemory_policy"] ?? "-",
    expiredKeys: num(stats["expired_keys"]),
    evictedKeys: num(stats["evicted_keys"]),
    pubsubChannels: num(stats["pubsub_channels"]),
    pubsubPatterns: num(stats["pubsub_patterns"]),
    keyspace,
  };
}

export function Dashboard() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [shards, setShards] = useState<ShardStat[]>([]);
  const [opsPerSec, setOpsPerSec] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const prev = useRef<{ total: number; at: number } | null>(null);

  useEffect(() => {
    let alive = true;
    const tick = async () => {
      try {
        const [info, shardList] = await Promise.all([api.info(), api.shards()]);
        if (!alive) return;
        const st = readStats(info);
        const now = Date.now();
        if (prev.current) {
          const dt = (now - prev.current.at) / 1000;
          if (dt > 0) setOpsPerSec((st.totalCommands - prev.current.total) / dt);
        }
        prev.current = { total: st.totalCommands, at: now };
        setStats(st);
        setShards(shardList.shards);
        setError(null);
      } catch (err) {
        if (alive) setError(String(err));
      }
    };
    void tick();
    const t = setInterval(() => void tick(), 2000);
    return () => {
      alive = false;
      clearInterval(t);
    };
  }, []);

  if (error && !stats) return <div className="banner banner-error">{error}</div>;
  if (!stats) return <p className="muted">Loading…</p>;

  const memPct = stats.maxmemory > 0 ? Math.min(100, (stats.usedMemory / stats.maxmemory) * 100) : null;
  const maxKeys = Math.max(1, ...shards.map((s) => s.keys));
  const maxMem = Math.max(1, ...shards.map((s) => s.mem_bytes));

  return (
    <div className="screen">
      <h2>Dashboard</h2>
      <div className="card-grid">
        <div className="card">
          <div className="card-label">Version</div>
          <div className="card-value">{stats.version}</div>
        </div>
        <div className="card">
          <div className="card-label">Uptime</div>
          <div className="card-value">{fmtUptime(stats.uptime)}</div>
        </div>
        <div className="card">
          <div className="card-label">Clients</div>
          <div className="card-value">
            {stats.clients}
            {stats.blockedClients > 0 && <span className="muted"> ({stats.blockedClients} blocked)</span>}
          </div>
        </div>
        <div className="card">
          <div className="card-label">Ops/sec</div>
          <div className="card-value">{opsPerSec === null ? "—" : opsPerSec.toFixed(0)}</div>
        </div>
        <div className="card">
          <div className="card-label">Expired keys</div>
          <div className="card-value">{stats.expiredKeys}</div>
        </div>
        <div className="card">
          <div className="card-label">Evicted keys</div>
          <div className="card-value">{stats.evictedKeys}</div>
        </div>
        <div className="card">
          <div className="card-label">Pub/sub</div>
          <div className="card-value">
            {stats.pubsubChannels} ch / {stats.pubsubPatterns} pat
          </div>
        </div>
        <div className="card">
          <div className="card-label">Eviction policy</div>
          <div className="card-value">{stats.maxmemoryPolicy}</div>
        </div>
      </div>

      <h3>Memory</h3>
      <div className="membar-row">
        <div className="membar">
          <div
            className={memPct !== null && memPct > 90 ? "membar-fill membar-hot" : "membar-fill"}
            style={{ width: `${memPct ?? (stats.usedMemory > 0 ? 100 : 0)}%` }}
          />
        </div>
        <span className="mono">
          {fmtBytes(stats.usedMemory)}
          {stats.maxmemory > 0 ? ` / ${fmtBytes(stats.maxmemory)}` : " (no maxmemory)"}
        </span>
      </div>

      {Object.keys(stats.keyspace).length > 0 && (
        <>
          <h3>Keyspace</h3>
          <div className="kv-grid">
            {Object.entries(stats.keyspace).map(([db, v]) => (
              <div key={db} className="kv">
                <span className="mono">{db}</span>
                <span className="muted">{v}</span>
              </div>
            ))}
          </div>
        </>
      )}

      <h3>Shards ({shards.length})</h3>
      <div className="shard-grid" style={{ gridTemplateColumns: `repeat(${Math.ceil(Math.sqrt(shards.length || 1))}, 1fr)` }}>
        {shards.map((s) => {
          const heat = s.keys / maxKeys;
          const memHeat = s.mem_bytes / maxMem;
          const bg = `rgba(${Math.round(40 + 180 * heat)}, ${Math.round(80 + 60 * (1 - heat))}, 120, ${0.25 + 0.55 * Math.max(heat, memHeat)})`;
          return (
            <div key={s.index} className="shard-cell" style={{ background: bg }} title={`shard ${s.index}: ${s.keys} keys, ${fmtBytes(s.mem_bytes)}, ${s.expires} expires, queue ${s.queue_depth}, heap ${s.expiry_heap}`}>
              <div className="shard-idx">#{s.index}</div>
              <div className="shard-keys">{s.keys}</div>
              <div className="shard-mem">{fmtBytes(s.mem_bytes)}</div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
