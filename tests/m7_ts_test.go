// M7b tests (design doc §11.2): drive the @ultima/client TypeScript
// round-trip (clients/typescript/test/roundtrip.ts) under bun against
// in-process servers — once against the plain WS+HTTP wiring (mirroring
// cmd/ultima-server without auth), once against the auth-enabled M6a
// wiring (newM6Env). Skipped gracefully when bun or the client's
// node_modules are absent.
package tests

import (
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/pschlump/ultima/lib/auth"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/httpapi"
	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/wssession"
	"github.com/pschlump/ultima/lib/wssrv"
)

// m7TSClientDir locates clients/typescript and skips when the toolchain or
// installed dependencies are missing.
func m7TSClientDir(t *testing.T) (bun string, dir string) {
	t.Helper()
	bun, err := exec.LookPath("bun")
	if err != nil {
		t.Skip("bun not on PATH; install bun to run the TypeScript client round-trip")
	}
	// `go test` runs with the package directory (tests/) as cwd.
	dir, err = filepath.Abs(filepath.Join("..", "clients", "typescript"))
	if err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(filepath.Join(dir, "node_modules")); err != nil || !st.IsDir() {
		t.Skip("clients/typescript/node_modules missing; run `bun install` in clients/typescript")
	}
	return bun, dir
}

// m7RunRoundTrip typechecks the client (strict tsc) and runs the bun
// round-trip script with the given environment.
func m7RunRoundTrip(t *testing.T, bun, dir string, env ...string) {
	t.Helper()

	tc := exec.Command(bun, "run", "typecheck")
	tc.Dir = dir
	if out, err := tc.CombinedOutput(); err != nil {
		t.Fatalf("@ultima/client typecheck failed: %v\n%s", err, out)
	}

	cmd := exec.Command(bun, filepath.Join("test", "roundtrip.ts"))
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.CombinedOutput()
	t.Logf("bun test/roundtrip.ts output:\n%s", out)
	if err != nil {
		t.Fatalf("@ultima/client round-trip failed: %v", err)
	}
}

// TestM7TSClientRoundTrip boots the plain WS+HTTP surfaces (no auth) and
// runs the full round-trip: typed + generic commands, pub/sub push,
// §9.4 session drop/resume with missed-push replay, SESSION_EXPIRED gap,
// and the REST data endpoints.
func TestM7TSClientRoundTrip(t *testing.T) {
	bun, dir := m7TSClientDir(t)

	shards := shard.NewEngine(0, 16)
	t.Cleanup(shards.Close)
	eng := commands.NewEngine(shards, "test", 0)
	reg := wssession.NewRegistry(eng, 0, 0, testLogger())
	t.Cleanup(reg.Close)

	r := chi.NewRouter()
	httpapi.NewServer(eng, nil, nil, testLogger(), nil).Register(r)
	r.Get("/ws/v1", wssrv.Handler(eng, nil, reg, testLogger(), nil))
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/v1"
	m7RunRoundTrip(t, bun, dir, "WS_URL="+wsURL, "HTTP_URL="+srv.URL)
}

// TestM7TSClientAuth runs the same round-trip against the M6a auth-enabled
// wiring: the script probes auth, verifies 401s without a token, logs in as
// the bootstrap admin, and re-runs every command with the Bearer token.
func TestM7TSClientAuth(t *testing.T) {
	bun, dir := m7TSClientDir(t)

	e := newM6Env(t)
	wsURL := "ws" + strings.TrimPrefix(e.httpURL, "http") + "/ws/v1"
	m7RunRoundTrip(t, bun, dir,
		"WS_URL="+wsURL,
		"HTTP_URL="+e.httpURL,
		"AUTH=1",
		"USER="+auth.BootstrapAdmin,
		"PASS=boot-pw",
	)
}
