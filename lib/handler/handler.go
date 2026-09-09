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

	"github.com/pschlump/ultima/lib/auth"
	"github.com/pschlump/ultima/lib/commands"
)

// Register wires the HTTP routes onto r. p is the M5c persistence
// manager (commands.Persister) behind the /api/v1/save family of
// triggers (§13.1); nil disables them (bare test servers). svc is the
// M6a auth service (§9): when non-nil, /api/v1/* is Bearer-gated
// (except the public /api/v1/auth/login and /api/v1/auth/refresh, which
// RegisterAuth adds); /health and /ready stay public either way (§10.1).
func Register(r chi.Router, p commands.Persister, svc *auth.Service) {
	r.Get("/health", health)
	r.Get("/ready", health)
	if svc == nil {
		registerAPI(r, p)
		return
	}
	RegisterAuth(r, svc)
	r.Group(func(r chi.Router) {
		r.Use(svc.RequireAuth)
		registerAPI(r, p)
	})
}

// registerAPI mounts the management endpoints (ungated caller routes).
func registerAPI(r chi.Router, p commands.Persister) {
	r.Get("/api/v1/ping", ping)
	if p != nil {
		r.Post("/api/v1/save", saveTrigger(p, false))
		r.Post("/api/v1/bgsave", saveTrigger(p, true))
		r.Post("/api/v1/bgrewriteaof", rewriteTrigger(p))
	}
}

// saveTrigger runs SAVE (block=false: synchronous whole-server dump) or
// BGSAVE (block=true: per-shard staggered background dump).
func saveTrigger(p commands.Persister, background bool) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		var err error
		if background {
			err = p.BGSave()
		} else {
			err = p.Save()
		}
		if err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"status": "error", "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}

// rewriteTrigger runs BGREWRITEAOF.
func rewriteTrigger(p commands.Persister) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if err := p.BGRewriteAOF(); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"status": "error", "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
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
