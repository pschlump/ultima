package main

import (
	"log/slog"
	"net"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/pschlump/ultima/lib/auth"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/httpapi"
	"github.com/pschlump/ultima/lib/wssession"
	"github.com/pschlump/ultima/lib/wssrv"
	"github.com/pschlump/ultima/web"
)

// newRouter builds the chi mux for the HTTP/WS port: lib/httpapi mounts
// the M6c management API (generated chi-server bindings from
// api/openapi.yaml, §10.1) with its middleware chain (request-id →
// logging → Recoverer → Timeout → Prometheus → JWT gate — real-IP is
// deliberately omitted, see lib/httpapi), the public /api/openapi.yaml +
// /api/docs routes, and the allowlist-guarded /metrics. The /ws/v1
// WebSocket command endpoint (lib/wssrv, §6.3) is mounted outside the API
// group so its Unwrap/Hijack contract is preserved. authSvc is the M6a
// auth service (nil when auth.enabled is false): it gates /api/v1/*
// behind Bearer tokens (§10.1) and the WS upgrade behind an access token
// (§9.3). reg is the M6b resumable-session registry (§9.4). metricsAllow
// is the parsed server.metrics_allow list.
func newRouter(logger *slog.Logger, eng *commands.Engine, p commands.Persister, authSvc *auth.Service, reg *wssession.Registry, metricsAllow []*net.IPNet, originAllow []string) http.Handler {
	r := chi.NewRouter()
	httpapi.NewServer(eng, p, authSvc, logger, metricsAllow).Register(r)
	r.Get("/ws/v1", wssrv.Handler(eng, authSvc, reg, logger, originAllow))
	// M6d web UI (§10.2): the embedded SPA catch-all comes last; chi's
	// exact/static routes above always win over the wildcard, and the
	// handler itself 404s unmatched machine paths (/api/, /ws/, …).
	r.Handle("/*", web.Handler())
	return r
}
