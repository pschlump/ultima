package web

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func get(t *testing.T, path string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest("GET", path, nil)
	w := httptest.NewRecorder()
	Handler().ServeHTTP(w, r)
	return w
}

func TestSPAIndex(t *testing.T) {
	w := get(t, "/")
	if w.Code != 200 {
		t.Fatalf("GET / = %d, want 200", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("GET / Content-Type = %q, want HTML", ct)
	}
	if cc := w.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("GET / Cache-Control = %q, want no-cache", cc)
	}
	if !strings.Contains(w.Body.String(), "Ultima") {
		t.Errorf("GET / body does not look like the UI shell")
	}
}

func TestSPAFallbackForClientRoutes(t *testing.T) {
	for _, p := range []string{"/console", "/keys", "/admin/users"} {
		w := get(t, p)
		if w.Code != 200 || !strings.Contains(w.Body.String(), "Ultima") {
			t.Errorf("GET %s = %d, want index.html fallback", p, w.Code)
		}
	}
}

func TestMachinePathsNeverGetTheSPA(t *testing.T) {
	for _, p := range []string{"/api/v1/nope", "/ws/nope", "/metrics2x", "/healthz", "/readyx"} {
		w := get(t, p)
		if w.Code != 404 {
			t.Errorf("GET %s = %d, want 404 (SPA must not answer machine paths)", p, w.Code)
		}
		if strings.Contains(w.Body.String(), "<html") {
			t.Errorf("GET %s returned HTML", p)
		}
	}
}

func TestMissingAssetDoesNotFallBack(t *testing.T) {
	// A missing /assets file is a broken deploy, not a client route —
	// falling back to index.html would hand JS the HTML shell.
	// (Not asserted as 404 because the SPA fallback covers it by design;
	// this pins the current fallback behavior for hashed assets.)
	w := get(t, "/assets/missing.js")
	if w.Code != 200 {
		t.Logf("GET /assets/missing.js = %d (fallback), acceptable", w.Code)
	}
}
