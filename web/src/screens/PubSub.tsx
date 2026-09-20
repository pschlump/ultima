// Pub/sub inspector: PUBSUB CHANNELS / NUMSUB via WS generic commands, a
// subscribe form (channel or pattern), and a live message stream riding the
// §9.4 resumable session (subscriptions retained across reconnects;
// re-subscribed automatically on SESSION_EXPIRED).
import { useCallback, useEffect, useRef, useState, type FormEvent } from "react";
import type { Value } from "../../../gen/ts/ultima/v1/command_pb";
import { getTokens } from "../lib/api";
import { UltimaWS, type WSState } from "../lib/ws";
import { valueToString } from "../components/ValueView";

interface Msg {
  id: number;
  kind: string;
  channel: string;
  pattern: string;
  payload: string;
  at: string;
}

function decodePush(v: Value): Omit<Msg, "id" | "at"> | null {
  if (v.kind.case !== "push") return null;
  const elems = v.kind.value.elems;
  const kind = elems[0] ? valueToString(elems[0]) : "";
  if (kind === "message") {
    return {
      kind,
      channel: elems[1] ? valueToString(elems[1]) : "",
      pattern: "",
      payload: elems[2] ? valueToString(elems[2]) : "",
    };
  }
  if (kind === "pmessage") {
    return {
      kind,
      pattern: elems[1] ? valueToString(elems[1]) : "",
      channel: elems[2] ? valueToString(elems[2]) : "",
      payload: elems[3] ? valueToString(elems[3]) : "",
    };
  }
  // subscribe/unsubscribe acks
  return {
    kind,
    channel: elems[1] ? valueToString(elems[1]) : "",
    pattern: "",
    payload: elems[2] ? valueToString(elems[2]) : "",
  };
}

interface ChannelStat {
  channel: string;
  subscribers: number;
}

export function PubSub() {
  const [state, setState] = useState<WSState>("connecting");
  const [channels, setChannels] = useState<ChannelStat[]>([]);
  const [messages, setMessages] = useState<Msg[]>([]);
  const [subs, setSubs] = useState<{ channels: string[]; patterns: string[] }>({ channels: [], patterns: [] });
  const [channelInput, setChannelInput] = useState("");
  const [patternInput, setPatternInput] = useState("");
  const [error, setError] = useState<string | null>(null);
  const wsRef = useRef<UltimaWS | null>(null);
  const idRef = useRef(0);
  const subsRef = useRef(subs);
  subsRef.current = subs;
  const outRef = useRef<HTMLDivElement | null>(null);

  const pushMsg = useCallback((m: Omit<Msg, "id" | "at">) => {
    idRef.current += 1;
    const id = idRef.current;
    setMessages((ms) => [...ms.slice(-499), { ...m, id, at: new Date().toLocaleTimeString() }]);
  }, []);

  const refreshChannels = useCallback(async () => {
    const ws = wsRef.current;
    if (!ws) return;
    try {
      const chans = await ws.exec("PUBSUB", ["CHANNELS"]);
      const names: string[] =
        chans.kind.case === "array" ? chans.kind.value.elems.map((e) => valueToString(e)) : [];
      const stats: ChannelStat[] = [];
      if (names.length > 0) {
        const numsub = await ws.exec("PUBSUB", ["NUMSUB", ...names]);
        if (numsub.kind.case === "map") {
          for (const p of numsub.kind.value.pairs) {
            stats.push({
              channel: valueToString(p.key),
              subscribers: Number(valueToString(p.value)),
            });
          }
        }
      }
      setChannels(stats);
      setError(null);
    } catch (err) {
      setError(String(err));
    }
  }, []);

  const subscribe = useCallback(
    async (command: "SUBSCRIBE" | "PSUBSCRIBE", name: string) => {
      const ws = wsRef.current;
      if (!ws || !name) return;
      const res = await ws.exec(command, [name]);
      if (res.kind.case === "error") {
        setError(res.kind.value);
        return;
      }
      setSubs((s) =>
        command === "SUBSCRIBE"
          ? { ...s, channels: [...new Set([...s.channels, name])] }
          : { ...s, patterns: [...new Set([...s.patterns, name])] },
      );
    },
    [],
  );

  const unsubscribe = useCallback(async (command: "UNSUBSCRIBE" | "PUNSUBSCRIBE", name: string) => {
    const ws = wsRef.current;
    if (!ws) return;
    await ws.exec(command, [name]);
    setSubs((s) =>
      command === "UNSUBSCRIBE"
        ? { ...s, channels: s.channels.filter((c) => c !== name) }
        : { ...s, patterns: s.patterns.filter((p) => p !== name) },
    );
  }, []);

  useEffect(() => {
    const ws = new UltimaWS(() => getTokens()?.accessToken ?? null, {
      onPush: (value) => {
        const m = decodePush(value);
        if (m) pushMsg(m);
      },
      onSessionReset: () => {
        // Subscriptions were lost with the session; re-subscribe.
        const s = subsRef.current;
        for (const c of s.channels) void ws.exec("SUBSCRIBE", [c]);
        for (const p of s.patterns) void ws.exec("PSUBSCRIBE", [p]);
        pushMsg({ kind: "note", channel: "", pattern: "", payload: "SESSION_EXPIRED — re-subscribed automatically" });
      },
      onStateChange: setState,
    });
    wsRef.current = ws;
    ws.connect();
    return () => {
      ws.close();
      wsRef.current = null;
    };
  }, [pushMsg]);

  useEffect(() => {
    outRef.current?.scrollTo({ top: outRef.current.scrollHeight });
  }, [messages]);

  const onSub = (e: FormEvent) => {
    e.preventDefault();
    void subscribe("SUBSCRIBE", channelInput.trim());
    setChannelInput("");
  };
  const onPSub = (e: FormEvent) => {
    e.preventDefault();
    void subscribe("PSUBSCRIBE", patternInput.trim());
    setPatternInput("");
  };

  return (
    <div className="screen">
      <h2>Pub/Sub</h2>
      {state === "reconnecting" && <div className="banner banner-warn">Reconnecting…</div>}
      {error && <div className="banner banner-error">{error}</div>}

      <div className="toolbar">
        <button onClick={() => void refreshChannels()}>Refresh channels</button>
      </div>
      <table>
        <thead>
          <tr>
            <th>channel</th>
            <th>subscribers</th>
          </tr>
        </thead>
        <tbody>
          {channels.map((c) => (
            <tr key={c.channel}>
              <td className="mono">{c.channel}</td>
              <td className="mono">{c.subscribers}</td>
            </tr>
          ))}
          {channels.length === 0 && (
            <tr>
              <td colSpan={2} className="muted">
                No active channels.
              </td>
            </tr>
          )}
        </tbody>
      </table>

      <h3>Subscriptions</h3>
      <div className="toolbar">
        <form onSubmit={onSub} className="toolbar">
          <input placeholder="channel" value={channelInput} onChange={(e) => setChannelInput(e.target.value)} />
          <button type="submit" disabled={!channelInput.trim()}>Subscribe</button>
        </form>
        <form onSubmit={onPSub} className="toolbar">
          <input placeholder="pattern (glob)" value={patternInput} onChange={(e) => setPatternInput(e.target.value)} />
          <button type="submit" disabled={!patternInput.trim()}>PSubscribe</button>
        </form>
      </div>
      <div className="tag-row">
        {subs.channels.map((c) => (
          <span key={c} className="tag">
            {c}{" "}
            <button className="tag-x" onClick={() => void unsubscribe("UNSUBSCRIBE", c)} title="unsubscribe">
              ×
            </button>
          </span>
        ))}
        {subs.patterns.map((p) => (
          <span key={p} className="tag tag-pattern">
            {p}{" "}
            <button className="tag-x" onClick={() => void unsubscribe("PUNSUBSCRIBE", p)} title="punsubscribe">
              ×
            </button>
          </span>
        ))}
        {subs.channels.length === 0 && subs.patterns.length === 0 && (
          <span className="muted">No subscriptions.</span>
        )}
      </div>

      <h3>Messages</h3>
      <div className="console-out" ref={outRef}>
        {messages.map((m) => (
          <div key={m.id} className="console-line mono">
            <span className="muted">{m.at}</span> <span className="vv-tag">{m.kind}</span>{" "}
            {m.pattern && <span className="muted">[{m.pattern}] </span>}
            {m.channel && <strong>{m.channel}</strong>} {m.payload && <span>{m.payload}</span>}
          </div>
        ))}
        {messages.length === 0 && <p className="muted">Subscribe to a channel or pattern to see messages here.</p>}
      </div>
    </div>
  );
}
