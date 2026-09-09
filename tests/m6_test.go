// M6a auth-core tests (design doc §9, D20): login/refresh/logout with
// rotation and theft detection, TOTP 2FA enrollment, administrative
// account management, and JWT enforcement on all three gated surfaces
// (HTTP API, gRPC call metadata, WebSocket upgrade). Everything runs
// against in-process servers with a temp-dir key pair and accounts file.
package tests

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/pschlump/htotp"
	ultimav1 "github.com/pschlump/ultima/gen/go/ultima/v1"
	"github.com/pschlump/ultima/lib/auth"
	"github.com/pschlump/ultima/lib/commands"
	"github.com/pschlump/ultima/lib/config"
	"github.com/pschlump/ultima/lib/grpcsrv"
	"github.com/pschlump/ultima/lib/handler"
	"github.com/pschlump/ultima/lib/shard"
	"github.com/pschlump/ultima/lib/wssrv"
)

// m6Env boots the full auth-enabled wiring (shard engine → command engine
// → auth service → chi mux + gRPC server) exactly as cmd/ultima-server
// does, on ephemeral listeners.
type m6Env struct {
	svc          *auth.Service
	httpURL      string
	grpcC        ultimav1.UltimaClient
	privPath     string
	pubPath      string
	accountsPath string
}

func newM6Env(t *testing.T) *m6Env {
	t.Helper()
	dir := t.TempDir()

	// Ed25519 pair, same PEM shapes bin/gen-jwt-keys.sh writes.
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

	accountsPath := filepath.Join(dir, "accounts.json")
	svc, err := auth.NewService(config.AuthConfig{
		Enabled:                true,
		JwtPrivateKeyFile:      privPath,
		JwtPublicKeyFile:       pubPath,
		AccessTokenTTL:         "15m",
		RefreshTokenTTL:        "720h",
		TotpIssuer:             "UltimaTest",
		TotpSkew:               1,
		AccountsFile:           accountsPath,
		BootstrapAdminPassword: "boot-pw",
	}, dir, testLogger())
	if err != nil {
		t.Fatalf("NewService: %s", err)
	}

	shards := shard.NewEngine(0, 16)
	t.Cleanup(shards.Close)
	eng := commands.NewEngine(shards, "test", 0)

	r := chi.NewRouter()
	handler.Register(r, nil, svc)
	r.Get("/ws/v1", wssrv.Handler(eng, svc, testLogger()))
	httpSrv := httptest.NewServer(r)
	t.Cleanup(httpSrv.Close)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcSrv := grpcsrv.New(eng, svc)
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.GracefulStop)
	t.Cleanup(func() { _ = lis.Close() })
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return &m6Env{
		svc:          svc,
		httpURL:      httpSrv.URL,
		grpcC:        ultimav1.NewUltimaClient(conn),
		privPath:     privPath,
		pubPath:      pubPath,
		accountsPath: accountsPath,
	}
}

// do performs one JSON HTTP round-trip and decodes the response body.
func (e *m6Env) do(t *testing.T, method, path, bearer string, body any) (int, map[string]any) {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rd = bytes.NewReader(raw)
	} else {
		rd = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, e.httpURL+path, rd)
	if err != nil {
		t.Fatal(err)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// login logs in as the bootstrap admin and returns the token pair.
func (e *m6Env) login(t *testing.T, user, pass, totp string) map[string]any {
	t.Helper()
	st, body := e.do(t, http.MethodPost, "/api/v1/auth/login", "",
		map[string]string{"username": user, "password": pass, "totp": totp})
	if st != http.StatusOK {
		t.Fatalf("login %s: status %d body %v", user, st, body)
	}
	return body
}

func TestM6HTTPAuthFlow(t *testing.T) {
	e := newM6Env(t)

	// Public endpoints stay open.
	if st, _ := e.do(t, http.MethodGet, "/health", "", nil); st != http.StatusOK {
		t.Fatalf("/health status %d", st)
	}

	// The management API is gated.
	if st, b := e.do(t, http.MethodGet, "/api/v1/ping", "", nil); st != http.StatusUnauthorized {
		t.Fatalf("ungated /api/v1/ping: status %d body %v", st, b)
	}

	pair := e.login(t, auth.BootstrapAdmin, "boot-pw", "")
	access := pair["access_token"].(string)
	refresh := pair["refresh_token"].(string)

	if st, b := e.do(t, http.MethodGet, "/api/v1/ping", access, nil); st != http.StatusOK || b["message"] != "PONG" {
		t.Fatalf("authed /api/v1/ping: status %d body %v", st, b)
	}

	// Refresh rotation: new pair, old refresh token dead; reuse = theft.
	st, body := e.do(t, http.MethodPost, "/api/v1/auth/refresh", "", map[string]string{"refresh_token": refresh})
	if st != http.StatusOK {
		t.Fatalf("refresh: status %d body %v", st, body)
	}
	refresh2 := body["refresh_token"].(string)
	if st, _ := e.do(t, http.MethodPost, "/api/v1/auth/refresh", "", map[string]string{"refresh_token": refresh}); st != http.StatusUnauthorized {
		t.Fatalf("reused refresh token: status %d, want 401", st)
	}
	// Theft revoked the family: the rotated token is dead too.
	if st, _ := e.do(t, http.MethodPost, "/api/v1/auth/refresh", "", map[string]string{"refresh_token": refresh2}); st != http.StatusUnauthorized {
		t.Fatalf("family should be revoked after theft: status %d, want 401", st)
	}

	// Logout revokes the family.
	pair = e.login(t, auth.BootstrapAdmin, "boot-pw", "")
	if st, _ := e.do(t, http.MethodPost, "/api/v1/auth/logout", pair["access_token"].(string),
		map[string]string{"refresh_token": pair["refresh_token"].(string)}); st != http.StatusOK {
		t.Fatalf("logout: status %d", st)
	}
	if st, _ := e.do(t, http.MethodPost, "/api/v1/auth/refresh", "", map[string]string{"refresh_token": pair["refresh_token"].(string)}); st != http.StatusUnauthorized {
		t.Fatalf("refresh after logout: status %d, want 401", st)
	}
}

func TestM6TOTPOverHTTP(t *testing.T) {
	e := newM6Env(t)
	pair := e.login(t, auth.BootstrapAdmin, "boot-pw", "")
	access := pair["access_token"].(string)

	st, enroll := e.do(t, http.MethodPost, "/api/v1/auth/totp/enable", access, nil)
	if st != http.StatusOK {
		t.Fatalf("totp enable: status %d body %v", st, enroll)
	}
	secret := enroll["secret"].(string)
	if enroll["provisioning_uri"] == "" || enroll["qr_code_png_base64"] == "" {
		t.Fatalf("enrollment missing uri/qr: %v", enroll)
	}

	code := htotp.NewDefaultTOTP(secret).Now()
	if st, b := e.do(t, http.MethodPost, "/api/v1/auth/totp/confirm", access, map[string]string{"totp": code}); st != http.StatusOK {
		t.Fatalf("totp confirm: status %d body %v", st, b)
	}

	// Login now demands the code.
	if st, b := e.do(t, http.MethodPost, "/api/v1/auth/login", "",
		map[string]string{"username": auth.BootstrapAdmin, "password": "boot-pw"}); st != http.StatusUnauthorized || b["error"] != "totp_required" {
		t.Fatalf("login without totp: status %d body %v, want 401 totp_required", st, b)
	}
	pair2 := e.login(t, auth.BootstrapAdmin, "boot-pw", htotp.NewDefaultTOTP(secret).Now())
	if pair2["access_token"] == "" {
		t.Fatal("login with totp returned no access token")
	}

	// Disable with password + current code.
	if st, b := e.do(t, http.MethodPost, "/api/v1/auth/totp/disable", pair2["access_token"].(string),
		map[string]string{"password": "boot-pw", "totp": htotp.NewDefaultTOTP(secret).Now()}); st != http.StatusOK {
		t.Fatalf("totp disable: status %d body %v", st, b)
	}
	e.login(t, auth.BootstrapAdmin, "boot-pw", "")
}

func TestM6AdminUserManagement(t *testing.T) {
	e := newM6Env(t)
	admin := e.login(t, auth.BootstrapAdmin, "boot-pw", "")
	access := admin["access_token"].(string)

	// Create a data user and a second admin.
	if st, b := e.do(t, http.MethodPost, "/api/v1/admin/users", access,
		map[string]string{"username": "dave", "password": "pw1", "class": "data"}); st != http.StatusCreated {
		t.Fatalf("create dave: status %d body %v", st, b)
	}
	if st, b := e.do(t, http.MethodPost, "/api/v1/admin/users", access,
		map[string]string{"username": "erin", "password": "pw2", "class": "admin"}); st != http.StatusCreated {
		t.Fatalf("create erin: status %d body %v", st, b)
	}

	// The data user's token works on the API but not on admin routes.
	dave := e.login(t, "dave", "pw1", "")
	daveAccess := dave["access_token"].(string)
	if st, _ := e.do(t, http.MethodGet, "/api/v1/ping", daveAccess, nil); st != http.StatusOK {
		t.Fatalf("dave ping: status %d", st)
	}
	if st, _ := e.do(t, http.MethodGet, "/api/v1/admin/users", daveAccess, nil); st != http.StatusForbidden {
		t.Fatalf("dave on admin route: status %d, want 403", st)
	}

	st, body := e.do(t, http.MethodGet, "/api/v1/admin/users", access, nil)
	if st != http.StatusOK {
		t.Fatalf("list users: status %d", st)
	}
	if users := body["users"].([]any); len(users) != 3 {
		t.Fatalf("expected 3 users, got %v", users)
	}

	// Last-admin guards: the bootstrap admin cannot be deleted, and erin
	// (soon the only other admin) cannot be disabled while admin lives...
	if st, _ := e.do(t, http.MethodDelete, "/api/v1/admin/users/"+auth.BootstrapAdmin, access, nil); st != http.StatusConflict {
		t.Fatalf("delete bootstrap admin: status %d, want 409", st)
	}
	// ...but with erin enabled, disabling the bootstrap admin is allowed.
	if st, b := e.do(t, http.MethodPut, "/api/v1/admin/users/"+auth.BootstrapAdmin, access,
		map[string]any{"disabled": true}); st != http.StatusOK {
		t.Fatalf("disable bootstrap admin: status %d body %v", st, b)
	}
	// erin's token still works; admin's outstanding token dies with the account.
	erin := e.login(t, "erin", "pw2", "")
	if st, _ := e.do(t, http.MethodGet, "/api/v1/admin/users", erin["access_token"].(string), nil); st != http.StatusOK {
		t.Fatalf("erin admin route: status %d", st)
	}
	if st, _ := e.do(t, http.MethodGet, "/api/v1/ping", access, nil); st != http.StatusUnauthorized {
		t.Fatalf("disabled admin's token should be dead: status %d, want 401", st)
	}

	// revoke-sessions kills dave's refresh family.
	if st, _ := e.do(t, http.MethodPost, "/api/v1/admin/users/dave/revoke-sessions", erin["access_token"].(string), nil); st != http.StatusOK {
		t.Fatalf("revoke-sessions: status %d", st)
	}
	if st, _ := e.do(t, http.MethodPost, "/api/v1/auth/refresh", "", map[string]string{"refresh_token": dave["refresh_token"].(string)}); st != http.StatusUnauthorized {
		t.Fatalf("dave refresh after revoke: status %d, want 401", st)
	}

	// dave can be deleted.
	if st, _ := e.do(t, http.MethodDelete, "/api/v1/admin/users/dave", erin["access_token"].(string), nil); st != http.StatusOK {
		t.Fatalf("delete dave: status %d", st)
	}
}

func TestM6PasswordChangeOverHTTP(t *testing.T) {
	e := newM6Env(t)
	pair := e.login(t, auth.BootstrapAdmin, "boot-pw", "")

	st, b := e.do(t, http.MethodPost, "/api/v1/auth/password", pair["access_token"].(string),
		map[string]string{"current_password": "boot-pw", "new_password": "new-pw"})
	if st != http.StatusOK {
		t.Fatalf("password change: status %d body %v", st, b)
	}
	// Sessions revoked: refresh dies; old password dies.
	if st, _ := e.do(t, http.MethodPost, "/api/v1/auth/refresh", "", map[string]string{"refresh_token": pair["refresh_token"].(string)}); st != http.StatusUnauthorized {
		t.Fatalf("refresh after password change: status %d, want 401", st)
	}
	if st, _ := e.do(t, http.MethodPost, "/api/v1/auth/login", "",
		map[string]string{"username": auth.BootstrapAdmin, "password": "boot-pw"}); st != http.StatusUnauthorized {
		t.Fatalf("old password: status %d, want 401", st)
	}
	e.login(t, auth.BootstrapAdmin, "new-pw", "")
}

func TestM6GRPCAuth(t *testing.T) {
	e := newM6Env(t)
	pair := e.login(t, auth.BootstrapAdmin, "boot-pw", "")

	// No token: Unauthenticated.
	if _, err := e.grpcC.ExecGeneric(context.Background(),
		&ultimav1.CommandRequest{Command: "PING"}); err == nil || !strings.Contains(err.Error(), "Unauthenticated") {
		t.Fatalf("unauthenticated ExecGeneric: err %v", err)
	}

	// Valid bearer token in call metadata works.
	ctx := metadata.AppendToOutgoingContext(context.Background(),
		"authorization", "bearer "+pair["access_token"].(string))
	resp, err := e.grpcC.ExecGeneric(ctx, &ultimav1.CommandRequest{Command: "PING"})
	if err != nil {
		t.Fatalf("authed ExecGeneric: %s", err)
	}
	if resp.GetReply().GetSimpleString() != "PONG" {
		t.Fatalf("PING reply: %v", resp.GetReply())
	}
}

func TestM6WSUpgradeAuth(t *testing.T) {
	e := newM6Env(t)
	pair := e.login(t, auth.BootstrapAdmin, "boot-pw", "")
	access := pair["access_token"].(string)
	wsBase := "ws" + strings.TrimPrefix(e.httpURL, "http") + "/ws/v1"

	// No token: the upgrade must be rejected.
	if _, resp, err := websocket.DefaultDialer.Dial(wsBase, nil); err == nil {
		t.Fatal("WS dial without token succeeded")
	} else if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("WS dial without token: status %v, want 401", resp)
	}

	// Query-param token works and commands flow.
	c, _, err := websocket.DefaultDialer.Dial(wsBase+"?access_token="+access, nil)
	if err != nil {
		t.Fatalf("WS dial with token: %s", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	replies := wsRoundTrip(t, c, wsGeneric("PING"))
	if replies[0].GetReply().GetSimpleString() != "PONG" {
		t.Fatalf("WS PING reply: %v", replies[0])
	}

	// Subprotocol form: `Sec-WebSocket-Protocol: bearer, <token>`.
	dialer := websocket.Dialer{Subprotocols: []string{"bearer", access}}
	c2, resp2, err := dialer.Dial(wsBase, nil)
	if err != nil {
		t.Fatalf("WS dial with subprotocol token: %s", err)
	}
	t.Cleanup(func() { _ = c2.Close() })
	if resp2.Header.Get("Sec-WebSocket-Protocol") != "bearer" {
		t.Fatalf("negotiated subprotocol = %q, want bearer", resp2.Header.Get("Sec-WebSocket-Protocol"))
	}
	replies = wsRoundTrip(t, c2, wsGeneric("PING"))
	if replies[0].GetReply().GetSimpleString() != "PONG" {
		t.Fatalf("WS PING via subprotocol: %v", replies[0])
	}
}

func TestM6AccountsSurviveReload(t *testing.T) {
	// One environment, then a second service over the same accounts file:
	// accounts and refresh families persist (covered in depth by
	// lib/auth's TestStorePersistenceAcrossReload; here at the wiring
	// level with the real config path).
	e := newM6Env(t)
	e.login(t, auth.BootstrapAdmin, "boot-pw", "")
	if _, err := e.svc.CreateAccount("frank", "pw5", auth.ClassData); err != nil {
		t.Fatal(err)
	}
	pair, err := e.svc.Login("frank", "pw5", "")
	if err != nil {
		t.Fatal(err)
	}

	svc2, err := auth.NewService(config.AuthConfig{
		Enabled:           true,
		JwtPrivateKeyFile: e.privPath,
		JwtPublicKeyFile:  e.pubPath,
		AccessTokenTTL:    "15m",
		RefreshTokenTTL:   "720h",
		TotpIssuer:        "UltimaTest",
		AccountsFile:      e.accountsPath,
	}, t.TempDir(), testLogger())
	if err != nil {
		t.Fatalf("reload NewService: %s", err)
	}
	if _, err := svc2.Login("frank", "pw5", ""); err != nil {
		t.Fatalf("login after reload: %s", err)
	}
	// Refresh families persist too: frank's pre-reload token rotates.
	if _, err := svc2.Refresh(pair.RefreshToken); err != nil {
		t.Fatalf("refresh after reload: %s", err)
	}
}
