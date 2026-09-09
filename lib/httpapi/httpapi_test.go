package httpapi

// M6c management-API tests (design doc §10.1, D7/D11): endpoint shapes
// over an in-test chi mux wired exactly like cmd/ultima-server/router.go
// (via Server.Register), the doc-drift guard between the embedded spec
// copy and the api/openapi.yaml source of truth, JsonBody validation,
// the /metrics IP allowlist, and the auth-gated flow with a real
// auth.Service (temp Ed25519 keys, as tests/m6_test.go does).

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/pschlump/ultima/lib/auth"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/config"
	"github.com/pschlump/ultima/lib/shard"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// newTestMux boots shard engine → command engine → Server.Register on a
// chi mux, mirroring the production wiring.
func newTestMux(t *testing.T, svc *auth.Service, p commands.Persister, metricsAllow []*net.IPNet) (*commands.Engine, http.Handler) {
	t.Helper()
	sh := shard.NewEngine(4, 16)
	t.Cleanup(sh.Close)
	eng := commands.NewEngine(sh, "test", 6379)
	r := chi.NewRouter()
	NewServer(eng, p, svc, testLogger(), metricsAllow).Register(r)
	return eng, r
}

// do performs one in-process HTTP round-trip and decodes a JSON body.
func do(t *testing.T, h http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
	st, raw := doRaw(t, h, method, path, body, "127.0.0.1:9")
	var out map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("%s %s: decoding %q: %s", method, path, raw, err)
		}
	}
	return st, out
}

func doRaw(t *testing.T, h http.Handler, method, path string, body any, remote string) (int, []byte) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rd = bytes.NewReader(raw)
	}
	req := httptest.NewRequest(method, path, rd)
	req.RemoteAddr = remote
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr.Code, rr.Body.Bytes()
}

// exec runs one engine command on a synthetic connection (test seeding).
func exec(eng *commands.Engine, db int, args ...string) {
	argv := make([][]byte, len(args))
	for i, a := range args {
		argv[i] = []byte(a)
	}
	eng.Execute(&commands.ConnState{Proto: 2, Authed: true, DB: db, Addr: "test"}, argv)
}

func TestPingHealthReady(t *testing.T) {
	_, h := newTestMux(t, nil, nil, nil)
	if st, b := do(t, h, http.MethodGet, "/api/v1/ping", nil); st != http.StatusOK || b["message"] != "PONG" {
		t.Fatalf("ping: status %d body %v", st, b)
	}
	for _, path := range []string{"/health", "/ready"} {
		if st, b := do(t, h, http.MethodGet, path, nil); st != http.StatusOK || b["status"] != "ok" {
			t.Fatalf("%s: status %d body %v", path, st, b)
		}
	}
}

// TestSpecMatchesContract is the D7 doc-drift guard: the embedded copy
// (served at /api/openapi.yaml) must equal the contract source of truth.
func TestSpecMatchesContract(t *testing.T) {
	src, err := os.ReadFile("../../api/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(src, openapiSpec) {
		t.Fatal("lib/httpapi/openapi.yaml drifts from api/openapi.yaml; run sh bin/gen-api.sh")
	}
}

func TestSpecAndDocsServed(t *testing.T) {
	_, h := newTestMux(t, nil, nil, nil)
	st, raw := doRaw(t, h, http.MethodGet, "/api/openapi.yaml", nil, "127.0.0.1:9")
	if st != http.StatusOK || !strings.Contains(string(raw), "openapi: 3.0.3") {
		t.Fatalf("/api/openapi.yaml: status %d", st)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/docs/", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), "<html") {
		t.Fatalf("/api/docs/: status %d", rr.Code)
	}
}

func TestInfoSections(t *testing.T) {
	eng, h := newTestMux(t, nil, nil, nil)
	exec(eng, 0, "SET", "k", "v")
	st, b := do(t, h, http.MethodGet, "/api/v1/info", nil)
	if st != http.StatusOK {
		t.Fatalf("info: status %d", st)
	}
	sections, ok := b["sections"].(map[string]any)
	if !ok {
		t.Fatalf("info: no sections object in %v", b)
	}
	server, ok := sections["server"].(map[string]any)
	if !ok || server["redis_version"] != commands.CompatVersion {
		t.Fatalf("info server section: %v", sections["server"])
	}
	ks, ok := sections["keyspace"].(map[string]any)
	if !ok || !strings.Contains(ks["db0"].(string), "keys=1") {
		t.Fatalf("info keyspace section: %v", sections["keyspace"])
	}
}

func TestShards(t *testing.T) {
	eng, h := newTestMux(t, nil, nil, nil)
	for i := range 20 {
		exec(eng, 0, "SET", "shk:"+string(rune('a'+i)), "v")
	}
	st, b := do(t, h, http.MethodGet, "/api/v1/shards", nil)
	if st != http.StatusOK {
		t.Fatalf("shards: status %d", st)
	}
	shards := b["shards"].([]any)
	if len(shards) != 4 {
		t.Fatalf("shards: got %d, want 4", len(shards))
	}
	total := 0.0
	for _, s := range shards {
		total += s.(map[string]any)["keys"].(float64)
	}
	if total != 20 {
		t.Fatalf("shard key total = %v, want 20", total)
	}
}

func TestClientsAndKill(t *testing.T) {
	eng, h := newTestMux(t, nil, nil, nil)
	cs := eng.NewConnState("1.2.3.4:5")
	cs.SetSurface("resp")

	st, b := do(t, h, http.MethodGet, "/api/v1/clients", nil)
	if st != http.StatusOK {
		t.Fatalf("clients: status %d", st)
	}
	clients := b["clients"].([]any)
	if len(clients) != 1 || clients[0].(map[string]any)["addr"] != "1.2.3.4:5" {
		t.Fatalf("clients: %v", clients)
	}
	id := uint64(clients[0].(map[string]any)["id"].(float64))

	// Unknown id → 404 not_found.
	if st, b := do(t, h, http.MethodPost, "/api/v1/clients/999999/kill", nil); st != http.StatusNotFound || b["error"] != "not_found" {
		t.Fatalf("kill unknown: status %d body %v", st, b)
	}
	// No kill hook → 409.
	if st, b := do(t, h, http.MethodPost, "/api/v1/clients/"+itoa(id)+"/kill", nil); st != http.StatusConflict || b["error"] != "client cannot be killed" {
		t.Fatalf("kill unkillable: status %d body %v", st, b)
	}
	// With a hook → 200 and the connection leaves the registry.
	cs.SetKillFunc(func() { eng.CloseConn(cs) })
	if st, b := do(t, h, http.MethodPost, "/api/v1/clients/"+itoa(id)+"/kill", nil); st != http.StatusOK || b["status"] != "ok" {
		t.Fatalf("kill: status %d body %v", st, b)
	}
	_, b = do(t, h, http.MethodGet, "/api/v1/clients", nil)
	if got := b["clients"].([]any); len(got) != 0 {
		t.Fatalf("clients after kill: %v", got)
	}
}

func itoa(v uint64) string {
	return strings.TrimSpace(jsonNumber(v))
}

func jsonNumber(v uint64) string {
	raw, _ := json.Marshal(v)
	return string(raw)
}

func TestSlowlog(t *testing.T) {
	eng, h := newTestMux(t, nil, nil, nil)
	// Log everything.
	if st, b := do(t, h, http.MethodPut, "/api/v1/config",
		map[string]any{"entries": map[string]string{"slowlog-log-slower-than": "0"}}); st != http.StatusOK {
		t.Fatalf("config set: status %d body %v", st, b)
	}
	exec(eng, 0, "SET", "slowkey", "v")
	st, b := do(t, h, http.MethodGet, "/api/v1/slowlog", nil)
	if st != http.StatusOK {
		t.Fatalf("slowlog: status %d", st)
	}
	entries := b["entries"].([]any)
	if len(entries) == 0 {
		t.Fatal("slowlog empty after commands with threshold 0")
	}
	e := entries[0].(map[string]any)
	if e["args"] == nil || e["duration_us"] == nil {
		t.Fatalf("slowlog entry shape: %v", e)
	}
	// RESET clears the ring; entries stays a (empty) JSON array.
	if st, b := do(t, h, http.MethodDelete, "/api/v1/slowlog", nil); st != http.StatusOK || b["status"] != "ok" {
		t.Fatalf("slowlog reset: status %d body %v", st, b)
	}
	_, raw := doRaw(t, h, http.MethodGet, "/api/v1/slowlog", nil, "127.0.0.1:9")
	if !strings.Contains(string(raw), `"entries":[]`) {
		t.Fatalf("slowlog after reset: %s", raw)
	}
}

func TestLatency(t *testing.T) {
	eng, h := newTestMux(t, nil, nil, nil)
	exec(eng, 0, "SET", "k", "v")
	st, b := do(t, h, http.MethodGet, "/api/v1/latency", nil)
	if st != http.StatusOK {
		t.Fatalf("latency: status %d", st)
	}
	cmds := b["commands"].([]any)
	if len(cmds) == 0 {
		t.Fatal("latency table empty after commands")
	}
}

func TestConfigGetPut(t *testing.T) {
	_, h := newTestMux(t, nil, nil, nil)
	st, b := do(t, h, http.MethodGet, "/api/v1/config", nil)
	if st != http.StatusOK {
		t.Fatalf("config get: status %d", st)
	}
	if _, ok := b["entries"].(map[string]any)["maxmemory"]; !ok {
		t.Fatalf("config get missing maxmemory: %v", b["entries"])
	}

	if st, b := do(t, h, http.MethodPut, "/api/v1/config",
		map[string]any{"entries": map[string]string{"maxmemory": "1048576"}}); st != http.StatusOK {
		t.Fatalf("config put: status %d body %v", st, b)
	}
	_, b = do(t, h, http.MethodGet, "/api/v1/config", nil)
	if got := b["entries"].(map[string]any)["maxmemory"]; got != "1048576" {
		t.Fatalf("maxmemory after put = %v", got)
	}

	// Bad value → 400 with the Redis error text; unknown param likewise.
	if st, b := do(t, h, http.MethodPut, "/api/v1/config",
		map[string]any{"entries": map[string]string{"maxmemory": "notanum"}}); st != http.StatusBadRequest || b["error"] == "" {
		t.Fatalf("config put bad value: status %d body %v", st, b)
	}
	if st, _ := do(t, h, http.MethodPut, "/api/v1/config",
		map[string]any{"entries": map[string]string{"no-such-param": "1"}}); st != http.StatusBadRequest {
		t.Fatalf("config put unknown param: status %d, want 400", st)
	}
	// Empty entries → 400.
	if st, _ := do(t, h, http.MethodPut, "/api/v1/config",
		map[string]any{"entries": map[string]string{}}); st != http.StatusBadRequest {
		t.Fatalf("config put empty: status %d, want 400", st)
	}

	// Restore unlimited memory for the rest of the suite.
	do(t, h, http.MethodPut, "/api/v1/config", map[string]any{"entries": map[string]string{"maxmemory": "0"}})
}

func TestFlushdb(t *testing.T) {
	eng, h := newTestMux(t, nil, nil, nil)
	exec(eng, 0, "SET", "f0", "v")
	exec(eng, 1, "SET", "f1", "v")

	if st, _ := do(t, h, http.MethodPost, "/api/v1/flushdb", map[string]any{"db": 1}); st != http.StatusOK {
		t.Fatalf("flushdb db=1: status %d", st)
	}
	if st, _ := do(t, h, http.MethodGet, "/api/v1/key/f1?db=1", nil); st != http.StatusNotFound {
		t.Fatalf("f1 after flushdb: status %d, want 404", st)
	}
	if st, _ := do(t, h, http.MethodGet, "/api/v1/key/f0", nil); st != http.StatusOK {
		t.Fatalf("f0 after flushdb db=1: status %d, want 200", st)
	}

	if st, _ := do(t, h, http.MethodPost, "/api/v1/flushdb", map[string]any{"all": true}); st != http.StatusOK {
		t.Fatalf("flushall: status %d", st)
	}
	if st, _ := do(t, h, http.MethodGet, "/api/v1/key/f0", nil); st != http.StatusNotFound {
		t.Fatalf("f0 after flushall: status %d, want 404", st)
	}
}

func TestScanKeys(t *testing.T) {
	eng, h := newTestMux(t, nil, nil, nil)
	want := map[string]bool{}
	for i := range 50 {
		k := "scan:" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		exec(eng, 0, "SET", k, "v")
		want[k] = true
	}

	// Cursor round-trip: walk until the cursor returns to "0".
	cursor := "0"
	seen := map[string]bool{}
	for {
		st, b := do(t, h, http.MethodGet, "/api/v1/keys/scan?cursor="+cursor+"&count=10", nil)
		if st != http.StatusOK {
			t.Fatalf("scan: status %d", st)
		}
		for _, k := range b["keys"].([]any) {
			seen[k.(string)] = true
		}
		cursor = b["cursor"].(string)
		if cursor == "0" {
			break
		}
	}
	for k := range want {
		if !seen[k] {
			t.Fatalf("scan missed %s", k)
		}
	}

	// MATCH narrows the result set.
	_, b := do(t, h, http.MethodGet, "/api/v1/keys/scan?match=scan:aa", nil)
	if got := b["keys"].([]any); len(got) != 1 || got[0] != "scan:aa" {
		t.Fatalf("scan match: %v", got)
	}
}

func TestKeyPreviewAndDelete(t *testing.T) {
	eng, h := newTestMux(t, nil, nil, nil)
	exec(eng, 0, "SET", "s", "hello")
	exec(eng, 0, "HSET", "h", "f1", "v1", "f2", "v2")
	exec(eng, 0, "RPUSH", "l", "a", "b", "c")
	exec(eng, 0, "SADD", "t", "x", "y")
	exec(eng, 0, "ZADD", "z", "1.5", "m1", "2", "m2")
	exec(eng, 0, "SET", "ttl", "v", "PX", "60000")

	get := func(key string) (int, map[string]any) {
		t.Helper()
		return do(t, h, http.MethodGet, "/api/v1/key/"+key, nil)
	}

	st, b := get("s")
	if st != http.StatusOK || b["type"] != "string" || b["value"] != "hello" || b["ttl_ms"].(float64) != -1 {
		t.Fatalf("string preview: status %d body %v", st, b)
	}
	_, b = get("h")
	if b["type"] != "hash" || b["value"].(map[string]any)["f1"] != "v1" {
		t.Fatalf("hash preview: %v", b)
	}
	_, b = get("l")
	if v := b["value"].([]any); b["type"] != "list" || len(v) != 3 || v[0] != "a" {
		t.Fatalf("list preview: %v", b)
	}
	_, b = get("t")
	if v := b["value"].([]any); b["type"] != "set" || len(v) != 2 {
		t.Fatalf("set preview: %v", b)
	}
	_, b = get("z")
	v := b["value"].([]any)
	if b["type"] != "zset" || len(v) != 2 {
		t.Fatalf("zset preview: %v", b)
	}
	m0 := v[0].(map[string]any)
	if m0["member"] != "m1" || m0["score"] != "1.5" {
		t.Fatalf("zset member shape: %v", m0)
	}
	_, b = get("ttl")
	if ms := b["ttl_ms"].(float64); ms <= 0 || ms > 60000 {
		t.Fatalf("ttl_ms = %v, want (0, 60000]", ms)
	}

	// Missing key → 404 not_found; DEL reports the deletion.
	if st, b := get("nosuch"); st != http.StatusNotFound || b["error"] != "not_found" {
		t.Fatalf("missing key: status %d body %v", st, b)
	}
	if st, b := do(t, h, http.MethodDelete, "/api/v1/key/s", nil); st != http.StatusOK || b["deleted"] != true {
		t.Fatalf("delete: status %d body %v", st, b)
	}
	if st, b := do(t, h, http.MethodDelete, "/api/v1/key/s", nil); st != http.StatusOK || b["deleted"] != false {
		t.Fatalf("re-delete: status %d body %v", st, b)
	}
}

func TestSaveFamilyNoPersister(t *testing.T) {
	_, h := newTestMux(t, nil, nil, nil)
	for _, path := range []string{"/api/v1/save", "/api/v1/bgsave", "/api/v1/bgrewriteaof"} {
		st, b := do(t, h, http.MethodPost, path, nil)
		if st != http.StatusServiceUnavailable || b["status"] != "error" || b["error"] != "persistence not configured" {
			t.Fatalf("%s: status %d body %v", path, st, b)
		}
	}
}

func TestMetricsAllowlist(t *testing.T) {
	_, h := newTestMux(t, nil, nil, nil)

	// Default (empty allowlist) is loopback-only: a spoofed remote is 403.
	st, b := doRaw(t, h, http.MethodGet, "/metrics", nil, "10.9.8.7:1234")
	if st != http.StatusForbidden {
		t.Fatalf("/metrics from 10.9.8.7: status %d body %s", st, b)
	}
	// Loopback passes and the engine metrics are in the exposition.
	st, raw := doRaw(t, h, http.MethodGet, "/metrics", nil, "127.0.0.1:1234")
	if st != http.StatusOK || !strings.Contains(string(raw), "# HELP ultima_") {
		t.Fatalf("/metrics from loopback: status %d", st)
	}
	for _, name := range []string{"ultima_uptime_seconds", "ultima_connected_clients", "ultima_memory_used_bytes", "ultima_slowlog_len"} {
		if !strings.Contains(string(raw), name) {
			t.Fatalf("/metrics missing %s", name)
		}
	}

	// An explicit allowlist admits its range and rejects the rest.
	_, n, _ := net.ParseCIDR("10.0.0.0/8")
	_, h2 := newTestMux(t, nil, nil, []*net.IPNet{n})
	if st, _ := doRaw(t, h2, http.MethodGet, "/metrics", nil, "10.1.2.3:4"); st != http.StatusOK {
		t.Fatalf("/metrics allowlisted: status %d", st)
	}
	if st, _ := doRaw(t, h2, http.MethodGet, "/metrics", nil, "127.0.0.1:4"); st != http.StatusForbidden {
		t.Fatalf("/metrics loopback outside allowlist: status %d, want 403", st)
	}
}

// newAuthService builds a real M6a service over temp Ed25519 keys and a
// temp accounts file (same shape as tests/m6_test.go's newM6Env).
func newAuthService(t *testing.T) *auth.Service {
	t.Helper()
	dir := t.TempDir()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privDER, _ := x509.MarshalPKCS8PrivateKey(priv)
	pubDER, _ := x509.MarshalPKIXPublicKey(pub)
	privPath := filepath.Join(dir, "jwt.pem")
	pubPath := filepath.Join(dir, "jwt.pub")
	if err := os.WriteFile(privPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pubPath, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), 0o644); err != nil {
		t.Fatal(err)
	}
	svc, err := auth.NewService(config.AuthConfig{
		Enabled:                true,
		JwtPrivateKeyFile:      privPath,
		JwtPublicKeyFile:       pubPath,
		AccessTokenTTL:         "15m",
		RefreshTokenTTL:        "720h",
		TotpIssuer:             "UltimaTest",
		TotpSkew:               1,
		AccountsFile:           filepath.Join(dir, "accounts.json"),
		BootstrapAdminPassword: "boot-pw",
	}, dir, testLogger())
	if err != nil {
		t.Fatalf("NewService: %s", err)
	}
	return svc
}

func TestAuthGateAndValidation(t *testing.T) {
	svc := newAuthService(t)
	_, h := newTestMux(t, svc, nil, nil)

	// Validation: an empty login body is now a 400 (JsonBody + validate
	// tags), not the pre-M6c 401 from the lenient manual decode.
	if st, b := do(t, h, http.MethodPost, "/api/v1/auth/login", map[string]any{}); st != http.StatusBadRequest || b["status"] != "error" {
		t.Fatalf("login {}: status %d body %v, want 400", st, b)
	}
	// The gate: the management API demands a token; probes stay public.
	if st, _ := do(t, h, http.MethodGet, "/api/v1/ping", nil); st != http.StatusUnauthorized {
		t.Fatalf("ungated ping: status %d, want 401", st)
	}
	if st, _ := do(t, h, http.MethodGet, "/health", nil); st != http.StatusOK {
		t.Fatalf("public /health: status %d", st)
	}

	// A real login works and the token opens the API.
	st, pair := do(t, h, http.MethodPost, "/api/v1/auth/login",
		map[string]string{"username": auth.BootstrapAdmin, "password": "boot-pw"})
	if st != http.StatusOK || pair["access_token"] == "" {
		t.Fatalf("login: status %d body %v", st, pair)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ping", nil)
	req.Header.Set("Authorization", "Bearer "+pair["access_token"].(string))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("authed ping: status %d body %s", rr.Code, rr.Body.Bytes())
	}
}
