// Package handler holds the HTTP surface (design doc §3): health endpoints
// and the M0 /api/v1/ping stub. The /ws/v1 WebSocket command endpoint
// lives in lib/wssrv (§6.3, M4); cmd/ultima-server/router.go mounts both
// onto the chi mux.
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
)

// Register wires the HTTP routes onto r.
func Register(r chi.Router) {
	r.Get("/health", health)
	r.Get("/ready", health)
	r.Get("/api/v1/ping", ping)
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
