// Client-library tests (M7a, §11.1): boot all three surfaces on ephemeral
// ports exactly as cmd/ultima-server wires them (the
// tests/integration_test.go pattern: lib/respserver + grpcsrv + chi with
// httpapi and wssrv over a wssession.Registry) and exercise the clients
// against them.
package ultima

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/pschlump/ultima/lib/auth"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/config"
	"github.com/pschlump/ultima/lib/grpcsrv"
	"github.com/pschlump/ultima/lib/httpapi"
	"github.com/pschlump/ultima/lib/respserver"
	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/wssession"
	"github.com/pschlump/ultima/lib/wssrv"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// testEnv is the three-surface test server plus its addresses.
type testEnv struct {
	eng      *commands.Engine
	svc      *auth.Service // nil unless withAuth
	respAddr string
	grpcAddr string
	httpAddr string // host:port (no scheme)
}

// newTestEnv boots everything; wsWindow overrides the §9.4 retention
// window (0 = the registry default).
func newTestEnv(t *testing.T, withAuth bool, wsWindow time.Duration) *testEnv {
	t.Helper()
	logger := testLogger()

	shards := shard.NewEngine(0, 16)
	t.Cleanup(shards.Close)
	eng := commands.NewEngine(shards, "test", 0)

	var svc *auth.Service
	if withAuth {
		svc = newAuthService(t, logger)
	}

	// RESP surface.
	respLis := listen(t)
	respSrv := respserver.New(respLis.Addr().String(), eng)
	go func() { _ = respSrv.Serve(respLis) }()
	t.Cleanup(func() { _ = respSrv.Close() })

	// gRPC surface.
	grpcLis := listen(t)
	grpcSrv := grpcsrv.New(eng, svc)
	go func() { _ = grpcSrv.Serve(grpcLis) }()
	t.Cleanup(grpcSrv.GracefulStop)

	// HTTP (REST management API + /ws/v1) surface.
	r := chi.NewRouter()
	httpapi.NewServer(eng, nil, svc, logger, nil).Register(r)
	reg := wssession.NewRegistry(eng, wsWindow, 0, logger)
	t.Cleanup(reg.Close)
	r.Get("/ws/v1", wssrv.Handler(eng, svc, reg, logger, nil))
	httpSrv := httptest.NewServer(r)
	t.Cleanup(httpSrv.Close)

	return &testEnv{
		eng:      eng,
		svc:      svc,
		respAddr: respLis.Addr().String(),
		grpcAddr: grpcLis.Addr().String(),
		httpAddr: strings.TrimPrefix(httpSrv.URL, "http://"),
	}
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	return lis
}

// newAuthService builds the M6a auth service exactly as tests/m6_test.go
// does: an Ed25519 pair in a temp dir, bootstrap admin password boot-pw.
func newAuthService(t *testing.T, logger *slog.Logger) *auth.Service {
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
	}, dir, logger)
	if err != nil {
		t.Fatalf("auth.NewService: %s", err)
	}
	return svc
}
