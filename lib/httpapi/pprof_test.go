package httpapi

// M9a /debug/pprof tests: the tree is absent unless server.pprof_enabled
// mounts it (404), the metrics_allow IP allowlist guards it exactly like
// /metrics, and with auth.enabled a Bearer access token is required too.

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/pschlump/ultima/lib/auth"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/shard"
)

// newPprofMux mirrors newTestMux but also mounts RegisterPprof, as
// cmd/ultima-server does when server.pprof_enabled is set.
func newPprofMux(t *testing.T, svc *auth.Service, metricsAllow []*net.IPNet) http.Handler {
	t.Helper()
	sh := shard.NewEngine(4, 16)
	t.Cleanup(sh.Close)
	eng := commands.NewEngine(sh, "test", 6379)
	r := chi.NewRouter()
	srv := NewServer(eng, nil, svc, testLogger(), metricsAllow)
	srv.Register(r)
	srv.RegisterPprof(r)
	return r
}

func TestPprofDisabled(t *testing.T) {
	_, h := newTestMux(t, nil, nil, nil)
	if st, _ := doRaw(t, h, http.MethodGet, "/debug/pprof/", nil, "127.0.0.1:9"); st != http.StatusNotFound {
		t.Fatalf("/debug/pprof/ unmounted: status %d, want 404", st)
	}
}

func TestPprofAllowlist(t *testing.T) {
	h := newPprofMux(t, nil, nil)

	// Default (empty allowlist) is loopback-only.
	for _, path := range []string{"/debug/pprof/", "/debug/pprof/cmdline", "/debug/pprof/goroutine?debug=1"} {
		if st, _ := doRaw(t, h, http.MethodGet, path, nil, "127.0.0.1:9"); st != http.StatusOK {
			t.Fatalf("GET %s from loopback: status %d, want 200", path, st)
		}
	}
	if st, _ := doRaw(t, h, http.MethodGet, "/debug/pprof/", nil, "10.9.8.7:1234"); st != http.StatusForbidden {
		t.Fatalf("/debug/pprof/ from 10.9.8.7: status %d, want 403", st)
	}

	// An explicit allowlist admits its range and rejects the rest.
	_, n, _ := net.ParseCIDR("10.0.0.0/8")
	h2 := newPprofMux(t, nil, []*net.IPNet{n})
	if st, _ := doRaw(t, h2, http.MethodGet, "/debug/pprof/", nil, "10.1.2.3:4"); st != http.StatusOK {
		t.Fatalf("/debug/pprof/ allowlisted: status %d, want 200", st)
	}
	if st, _ := doRaw(t, h2, http.MethodGet, "/debug/pprof/", nil, "127.0.0.1:4"); st != http.StatusForbidden {
		t.Fatalf("/debug/pprof/ loopback outside allowlist: status %d, want 403", st)
	}
}

func TestPprofRequiresAuth(t *testing.T) {
	svc := newAuthService(t)
	h := newPprofMux(t, svc, nil)

	// IP guard passes (loopback) but the JWT gate demands a token.
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	req.RemoteAddr = "127.0.0.1:9"
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("/debug/pprof/ without token: status %d, want 401", rr.Code)
	}

	st, pair := do(t, h, http.MethodPost, "/api/v1/auth/login",
		map[string]string{"username": auth.BootstrapAdmin, "password": "boot-pw"})
	if st != http.StatusOK || pair["access_token"] == "" {
		t.Fatalf("login: status %d body %v", st, pair)
	}
	req = httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	req.RemoteAddr = "127.0.0.1:9"
	req.Header.Set("Authorization", "Bearer "+pair["access_token"].(string))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("/debug/pprof/ with token: status %d body %s", rr.Code, rr.Body.Bytes())
	}
}
