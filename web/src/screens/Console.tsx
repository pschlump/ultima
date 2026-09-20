// Interactive console over /ws/v1 (design doc §6.3): input line, scrolling
// output (newest last), ↑/↓ history. SUBSCRIBE/PSUBSCRIBE replies and the
// subsequent pushes stream live; the §9.4 resumable session replays missed
// pushes across reconnects. A banner shows reconnect state and
// SESSION_EXPIRED resets.
import { useCallback, useEffect, useRef, useState, type KeyboardEvent } from "react";
import type { Value } from "../../../gen/ts/ultima/v1/command_pb";
import { getTokens } from "../lib/api";
import { parseLine } from "../lib/cmdline";
import { UltimaWS, type WSState } from "../lib/ws";
import { ValueView } from "../components/ValueView";

interface Line {
  id: number;
  kind: "input" | "reply" | "push" | "note" | "error";
  text?: string;
  value?: Value;
}

export function Console() {
  const [lines, setLines] = useState<Line[]>([]);
  const [input, setInput] = useState("");
  const [state, setState] = useState<WSState>("connecting");
  const [sessionExpired, setSessionExpired] = useState(false);
  const wsRef = useRef<UltimaWS | null>(null);
  const historyRef = useRef<string[]>([]);
  const histIdxRef = useRef(-1);
  const idRef = useRef(0);
  const outRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);

  const append = useCallback((line: Omit<Line, "id">) => {
    idRef.current += 1;
    const id = idRef.current;
    setLines((ls) => [...ls.slice(-999), { ...line, id }]);
  }, []);

  useEffect(() => {
    const ws = new UltimaWS(() => getTokens()?.accessToken ?? null, {
      onPush: (value) => append({ kind: "push", value }),
      onSessionReset: () => {
        setSessionExpired(true);
        append({ kind: "note", text: "SESSION_EXPIRED — the session was lost; re-issue SUBSCRIBE commands." });
      },
      onResumed: () => {
        setSessionExpired(false);
        append({ kind: "note", text: "Reconnected — session resumed; the server retained your subscriptions." });
      },
      onStateChange: setState,
    });
    wsRef.current = ws;
    ws.connect();
    append({ kind: "note", text: "Ultima console — RESP command lines over /ws/v1. Try: SET foo bar · GET foo · SUBSCRIBE chan" });
    return () => {
      ws.close();
      wsRef.current = null;
    };
  }, [append]);

  useEffect(() => {
    outRef.current?.scrollTo({ top: outRef.current.scrollHeight });
  }, [lines]);

  const run = async (line: string) => {
    const parsed = parseLine(line);
    if (!parsed) return;
    append({ kind: "input", text: line });
    historyRef.current.push(line);
    histIdxRef.current = -1;
    const ws = wsRef.current;
    if (!ws) return;
    try {
      const value = await ws.exec(parsed.command, parsed.args);
      append({ kind: "reply", value });
    } catch (err) {
      append({ kind: "error", text: String(err) });
    }
  };

  const onKey = (e: KeyboardEvent<HTMLInputElement>) => {
    const hist = historyRef.current;
    if (e.key === "Enter") {
      const line = input;
      setInput("");
      run(line).catch((err) => append({ kind: "error", text: String(err) }));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      if (hist.length === 0) return;
      const idx = histIdxRef.current === -1 ? hist.length - 1 : Math.max(0, histIdxRef.current - 1);
      histIdxRef.current = idx;
      setInput(hist[idx] ?? "");
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      if (histIdxRef.current === -1) return;
      const idx = histIdxRef.current + 1;
      if (idx >= hist.length) {
        histIdxRef.current = -1;
        setInput("");
      } else {
        histIdxRef.current = idx;
        setInput(hist[idx] ?? "");
      }
    }
  };

  return (
    <div className="screen console-screen">
      <h2>Console</h2>
      {state === "reconnecting" && (
        <div className="banner banner-warn">Connection lost — reconnecting…</div>
      )}
      {sessionExpired && state === "open" && (
        <div className="banner banner-error">Session expired — subscriptions were dropped; re-subscribe.</div>
      )}
      <div className="console-out" ref={outRef} onClick={() => inputRef.current?.focus()}>
        {lines.map((l) => (
          <div key={l.id} className={`console-line console-${l.kind}`}>
            {l.kind === "input" && <span className="console-prompt">ultima&gt; {l.text}</span>}
            {l.kind === "note" && <span className="muted">{l.text}</span>}
            {l.kind === "error" && <span className="vv-error">{l.text}</span>}
            {(l.kind === "reply" || l.kind === "push") && l.value && (
              <>
                {l.kind === "push" && <span className="vv-tag">push </span>}
                <ValueView value={l.value} />
              </>
            )}
          </div>
        ))}
      </div>
      <input
        ref={inputRef}
        className="console-in"
        value={input}
        onChange={(e) => setInput(e.target.value)}
        onKeyDown={onKey}
        placeholder="command arg arg … (↑/↓ history)"
        autoFocus
        spellCheck={false}
      />
    </div>
  );
}
