package httpapi

// net/http/pprof under /debug/pprof/ (M9a, design doc §14.2 — pprof
// profiles attach to benchmark reports). Mounted only when
// server.pprof_enabled; every profile endpoint is guarded by the
// metrics_allow IP allowlist (same posture as GET /metrics) plus a Bearer
// access token when auth is enabled (§10.1 gate).

import (
	"net"
	"net/http"
	"net/http/pprof"

	"github.com/go-chi/chi/v5"
)

// RegisterPprof mounts the profiling endpoints on r. The caller mounts
// them only when server.pprof_enabled is set — an unmounted /debug/pprof
// tree falls through to the SPA catch-all's machine-path 404.
func (s *Server) RegisterPprof(r chi.Router) {
	r.Route("/debug/pprof", func(r chi.Router) {
		r.Use(s.pprofGuard)
		r.Get("/", pprof.Index)
		r.Get("/cmdline", pprof.Cmdline)
		r.Get("/profile", pprof.Profile)
		r.Get("/symbol", pprof.Symbol)
		r.Post("/symbol", pprof.Symbol)
		r.Get("/trace", pprof.Trace)
		r.Get("/{name}", func(w http.ResponseWriter, req *http.Request) {
			pprof.Handler(chi.URLParam(req, "name")).ServeHTTP(w, req)
		})
	})
}

// pprofGuard enforces the metrics_allow IP allowlist first (cheap, and
// keeps profile surfaces off untrusted networks even when auth is off),
// then a valid Bearer token when auth.enabled.
func (s *Server) pprofGuard(next http.Handler) http.Handler {
	h := next
	if s.svc != nil {
		h = s.svc.RequireAuth(h)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		ip := net.ParseIP(host)
		if ip == nil || !s.metricsAllowed(ip) {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		h.ServeHTTP(w, r)
	})
}
