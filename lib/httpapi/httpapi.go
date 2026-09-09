// Package httpapi implements the Ultima management HTTP API (design doc
// §10.1, decisions D7/D11): the oapi-codegen chi-server bindings generated
// from api/openapi.yaml (gen/httpapi, the contract source of truth) are
// served by Server, request DTOs are validated through JsonBody
// (go-playground/validator over the generated validate: tags), and the
// middleware chain follows §10.1: request-id → request logging →
// Recoverer → Timeout → Prometheus → JWT auth (the real-IP step is
// deliberately omitted — see Register). The OpenAPI spec is embedded and
// served at /api/openapi.yaml with Swagger UI at /api/docs (both public);
// /metrics is guarded by an IP allowlist instead of JWT.
//
// The command surface reuses the one command engine (D3): handlers run
// INFO/CONFIG/SCAN/… through commands.Engine.Execute on a synthetic,
// unregistered ConnState (see syntheticConn).
package httpapi

import (
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"

	httpapigen "github.com/pschlump/ultima/gen/httpapi"
	"github.com/pschlump/ultima/lib/auth"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/handler"
)

// Server implements httpapigen.ServerInterface against the command engine.
// p is the M5c persistence manager behind the /api/v1/save family (nil →
// those endpoints reply 503). svc is the M6a auth service (nil → the API
// is fully open, the pre-M6 behavior). metricsAllow is the parsed form of
// the server.metrics_allow config list guarding /metrics (empty →
// loopback only).
type Server struct {
	eng          *commands.Engine
	persist      commands.Persister
	svc          *auth.Service
	logger       *slog.Logger
	metricsAllow []*net.IPNet

	registry *prometheus.Registry
	reqs     *prometheus.CounterVec
	durs     *prometheus.HistogramVec
}

// NewServer builds the management API server. metricsAllow entries are
// CIDRs (bare IPs already expanded to /32 or /128 by the caller).
func NewServer(eng *commands.Engine, p commands.Persister, svc *auth.Service, logger *slog.Logger, metricsAllow []*net.IPNet) *Server {
	s := &Server{
		eng:          eng,
		persist:      p,
		svc:          svc,
		logger:       logger,
		metricsAllow: metricsAllow,
		registry:     prometheus.NewRegistry(),
		reqs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ultima_http_requests_total",
			Help: "HTTP requests served by the management surface.",
		}, []string{"method", "path", "code"}),
		durs: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "ultima_http_request_duration_seconds",
			Help:    "HTTP request latency on the management surface.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "path"}),
	}
	s.registry.MustRegister(s.reqs, s.durs, &engineCollector{eng: eng})
	return s
}

// Register mounts the whole HTTP management surface onto r (§10.1):
// root middleware, the public spec/docs routes, and the generated API mux
// under Timeout → Prometheus → auth gate. /ws/v1 is mounted separately by
// the caller (lib/wssrv) so it keeps its Unwrap/Hijack contract outside
// the Timeout and Prometheus middleware. §10.1's real-IP step is omitted:
// chi's middleware.RealIP is deprecated (IP spoofing, GHSA-3fxj-6jh8-hvhx)
// and there is no trusted-proxy config yet — and mutating r.RemoteAddr
// from client headers would defeat the /metrics IP allowlist.
func (s *Server) Register(r chi.Router) {
	r.Use(middleware.RequestID)
	r.Use(handler.RequestLogger(s.logger))
	r.Use(middleware.Recoverer)

	// Public, non-API: the contract spec and its Swagger UI (D7).
	r.Get("/api/openapi.yaml", s.serveSpec)
	r.Mount("/api/docs", s.swaggerUI())

	r.Group(func(r chi.Router) {
		r.Use(middleware.Timeout(60 * time.Second))
		r.Use(s.PromMiddleware)
		r.Use(s.authGate)
		httpapigen.HandlerWithOptions(s, httpapigen.ChiServerOptions{
			BaseRouter:       r,
			ErrorHandlerFunc: paramErrorHandler,
		})
	})
}

// paramErrorHandler renders oapi-codegen parameter-binding failures (bad
// path/query types) as the contract's JSON Error shape instead of the
// default plain-text 400.
func paramErrorHandler(w http.ResponseWriter, _ *http.Request, err error) {
	writeError(w, http.StatusBadRequest, err.Error())
}

// authGate is the §10.1/§9.3 JWT gate: public probes, login/refresh,
// /metrics (IP-allowlist-guarded inside the handler) and the spec/docs
// routes stay open; /api/v1/admin/* requires the admin class; everything
// else requires a valid Bearer token. With no auth service configured
// (auth.enabled off) every route is open.
func (s *Server) authGate(next http.Handler) http.Handler {
	if s.svc == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case p == "/health" || p == "/ready" || p == "/metrics" ||
			p == "/api/v1/auth/login" || p == "/api/v1/auth/refresh" ||
			p == "/api/openapi.yaml" || strings.HasPrefix(p, "/api/docs"):
			next.ServeHTTP(w, r)
		case strings.HasPrefix(p, "/api/v1/admin/"):
			s.svc.RequireAdmin(next).ServeHTTP(w, r)
		default:
			s.svc.RequireAuth(next).ServeHTTP(w, r)
		}
	})
}

// syntheticConn builds the unregistered ConnState management endpoints
// run engine commands on (same pattern as AOF replay in
// lib/persist/manager.go): authed, RESP2 semantics, carrying the HTTP
// caller's JWT identity when the auth gate put one in the context.
// Synthetic conns deliberately do NOT go through Engine.NewConnState —
// they must not appear in the client registry (CLIENT LIST /clients).
func (s *Server) syntheticConn(r *http.Request, db int) *commands.ConnState {
	cs := &commands.ConnState{Proto: 2, Authed: true, DB: db, Addr: "http-api"}
	if id, ok := auth.IdentityFrom(r.Context()); ok {
		cs.User = id.Username
	}
	return cs
}

// writeJSON emits one JSON reply with the given status.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// writeError emits the contract's Error shape: {"status":"error","error":msg}.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, httpapigen.Error{Status: "error", Error: msg})
}

// statusOK is the common {"status":"ok"} reply.
func statusOK(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, httpapigen.Status{Status: "ok"})
}
