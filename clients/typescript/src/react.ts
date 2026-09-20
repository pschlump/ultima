// React bindings for @ultima/client (§11.2). This is the only module that
// imports react; consumers import it via the "@ultima/client/react" subpath.
import { useEffect, useRef, useState } from "react";
import { UltimaClient, type UltimaClientOptions } from "./client";
import type { PubSubMessage, PushHandler, WSState } from "./ws";

export interface UseUltimaResult {
  client: UltimaClient;
  /** Live WS connection state. */
  state: WSState;
}

/**
 * Create (once per component) an UltimaClient, connect on mount, close on
 * unmount. `options` is captured on the first render — pass a stable object.
 */
export function useUltima(options: UltimaClientOptions = {}): UseUltimaResult {
  const [state, setState] = useState<WSState>("connecting");
  const ref = useRef<UltimaClient | null>(null);
  if (ref.current === null) {
    const userOnState = options.ws?.onStateChange;
    ref.current = new UltimaClient({
      ...options,
      ws: {
        ...options.ws,
        onStateChange: (s) => {
          userOnState?.(s);
          setState(s);
        },
      },
    });
  }
  const client = ref.current;

  useEffect(() => {
    client.connect();
    return () => client.close();
  }, [client]);

  return { client, state };
}

/**
 * Subscribe to a channel (or glob pattern with { pattern: true }) while the
 * component is mounted; unsubscribes on cleanup. The latest handler closure
 * is always used — no resubscription churn when it changes identity.
 */
export function useSubscription(
  client: UltimaClient | null,
  channel: string,
  handler: PushHandler,
  options?: { pattern?: boolean },
): void {
  const handlerRef = useRef(handler);
  handlerRef.current = handler;
  const pattern = options?.pattern === true;

  useEffect(() => {
    if (!client) return;
    const h: PushHandler = (msg: PubSubMessage) => handlerRef.current(msg);
    const sub = pattern ? client.ws.psubscribe(channel, h) : client.ws.subscribe(channel, h);
    sub.catch(() => {
      // The WS layer re-subscribes on reconnect; a failed first attempt
      // surfaces through the client's onError handler.
    });
    return () => {
      const un = pattern ? client.ws.punsubscribe(channel, h) : client.ws.unsubscribe(channel, h);
      un.catch(() => {});
    };
  }, [client, channel, pattern]);
}
