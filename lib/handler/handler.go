// Package handler holds the HTTP/WebSocket surface (design doc §3, §6.3):
// health endpoints, the M0 /api/v1/ping stub, and the /ws/v1 WebSocket
// stub. cmd/ultima-server/router.go mounts these onto the chi mux.
package handler

import (
	"bufio"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
)

// Register wires the M0 HTTP and WS routes onto r.
func Register(r chi.Router, logger *slog.Logger) {
	r.Get("/health", health)
	r.Get("/ready", health)
	r.Get("/api/v1/ping", ping)
	r.Get("/ws/v1", wsStub(logger))
}

func health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func ping(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"message": "PONG"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// wsStub accepts a WebSocket upgrade and answers each text frame "PING"
// with "PONG" (anything else gets an error frame). Binary frames carry the
// protobuf Command envelope starting with M4 (§6.3).
func wsStub(logger *slog.Logger) http.HandlerFunc {
	upgrader := websocket.Upgrader{
		// M0 stub: no origin policy until the auth milestone (M6) lands.
		CheckOrigin: func(*http.Request) bool { return true },
	}
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logger.Warn("ws: upgrade failed", "err", err)
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			mt, payload, err := conn.ReadMessage()
			if err != nil {
				return // client went away or sent a close frame
			}
			if mt != websocket.TextMessage {
				if err := conn.WriteMessage(websocket.TextMessage, []byte("ERR only text frames supported in M0")); err != nil {
					return
				}
				continue
			}
			reply := "ERR unknown message"
			if string(payload) == "PING" {
				reply = "PONG"
			}
			if err := conn.WriteMessage(websocket.TextMessage, []byte(reply)); err != nil {
				return
			}
		}
	}
}

// statusRecorder captures the response status for request logging.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.status = code
	sr.ResponseWriter.WriteHeader(code)
}

// Unwrap lets middleware chains (e.g. the WS upgrader via
// http.ResponseController) reach the underlying ResponseWriter.
func (sr *statusRecorder) Unwrap() http.ResponseWriter { return sr.ResponseWriter }

// Hijack forwards to the underlying ResponseWriter; gorilla/websocket
// v1.5.3 type-asserts http.Hijacker directly during the upgrade.
func (sr *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := sr.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("handler: underlying ResponseWriter is not a http.Hijacker")
	}
	return h.Hijack()
}

// RequestLogger logs one JSON line per request through slog.
func RequestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sr := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(sr, r)
			logger.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", sr.status,
				"duration_ms", time.Since(start).Milliseconds(),
				"remote", r.RemoteAddr,
			)
		})
	}
}
