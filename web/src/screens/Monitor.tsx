// Monitor: generic MONITOR over its own UltimaWS (M6d adds the MONITOR
// command to the command engine; events stream back as push frames),
// streaming event list with pause/clear; reconnect notice on close.
import { useEffect, useRef, useState } from "react";
import { getTokens } from "../lib/api";
import { UltimaWS, type WSState } from "../lib/ws";
import { valueToString } from "../components/ValueView";

interface Event {
  id: number;
  text: string;
}

export function Monitor() {
  const [events, setEvents] = useState<Event[]>([]);
  const [paused, setPaused] = useState(false);
  const [state, setState] = useState<WSState>("connecting");
  const [notice, setNotice] = useState<string | null>(null);
  const pausedRef = useRef(false);
  const idRef = useRef(0);
  const outRef = useRef<HTMLDivElement | null>(null);
  pausedRef.current = paused;

  useEffect(() => {
    const ws = new UltimaWS(() => getTokens()?.accessToken ?? null, {
      onPush: (value) => {
        if (pausedRef.current) return;
        idRef.current += 1;
        const ev: Event = { id: idRef.current, text: valueToString(value) };
        setEvents((es) => [...es.slice(-1999), ev]);
      },
      onSessionReset: () => setNotice("Session expired — MONITOR stream restarted on a fresh session."),
      onResumed: () => setNotice(null),
      onStateChange: (s) => {
        setState(s);
        if (s === "reconnecting") setNotice("Connection lost — reconnecting…");
        if (s === "closed") setNotice("Connection closed.");
      },
    });
    ws.connect();
    ws.exec("monitor", [])
      .then((v) => {
        if (v.kind.case === "error") setNotice(v.kind.value);
        else setNotice(null);
      })
      .catch((err) => setNotice(String(err)));
    return () => ws.close();
  }, []);

  useEffect(() => {
    outRef.current?.scrollTo({ top: outRef.current.scrollHeight });
  }, [events]);

  return (
    <div className="screen console-screen">
      <h2>Monitor</h2>
      <div className="toolbar">
        <button onClick={() => setPaused((p) => !p)}>{paused ? "Resume" : "Pause"}</button>
        <button onClick={() => setEvents([])}>Clear</button>
        <span className="muted">{events.length} events{paused ? " (paused — incoming events dropped)" : ""}</span>
      </div>
      {notice && state !== "open" && <div className="banner banner-warn">{notice}</div>}
      {notice && state === "open" && <div className="banner banner-info">{notice}</div>}
      <div className="console-out" ref={outRef}>
        {events.map((e) => (
          <div key={e.id} className="console-line mono">
            {e.text}
          </div>
        ))}
        {events.length === 0 && <p className="muted">Waiting for commands… every command executed on the server appears here.</p>}
      </div>
    </div>
  );
}
