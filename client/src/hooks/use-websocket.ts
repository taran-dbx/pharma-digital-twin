import { useEffect, useRef, useState, useCallback } from "react";
import type { WsMessage } from "@/lib/types";

interface UseWebSocketOptions {
  onMessage?: (msg: WsMessage) => void;
  enabled?: boolean;
  industry?: string | null;
}

/**
 * Real-time data hook using Server-Sent Events (SSE).
 *
 * Named `useWebSocket` for backward compatibility so that existing imports
 * throughout the app continue to work unchanged.  Internally it connects to
 * the `/api/events` SSE endpoint instead of a WebSocket.
 */
function eventsUrl(industry?: string | null): string {
  if (!industry) return "/api/events";
  return `/api/events?industry=${encodeURIComponent(industry)}`;
}

export function useWebSocket({ onMessage, enabled = true, industry }: UseWebSocketOptions = {}) {
  const [connected, setConnected] = useState(false);
  const esRef = useRef<EventSource | null>(null);
  const reconnectTimer = useRef<ReturnType<typeof setTimeout>>();
  const onMessageRef = useRef(onMessage);
  onMessageRef.current = onMessage;

  const connect = useCallback(() => {
    if (!enabled) return;

    const es = new EventSource(eventsUrl(industry));
    esRef.current = es;

    es.onopen = () => {
      setConnected(true);
    };

    es.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data) as WsMessage;
        onMessageRef.current?.(data);
      } catch {
        // ignore malformed messages
      }
    };

    es.onerror = () => {
      setConnected(false);
      es.close();
      esRef.current = null;
      // EventSource auto-reconnects on transient errors, but once closed
      // we retry manually after a short delay.
      reconnectTimer.current = setTimeout(connect, 2000);
    };
  }, [enabled, industry]);

  useEffect(() => {
    connect();
    return () => {
      clearTimeout(reconnectTimer.current);
      esRef.current?.close();
    };
  }, [connect]);

  return { connected };
}
