package ultima

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pschlump/ultima/lib/auth"
	"github.com/pschlump/ultima/lib/resp"
)

// TestTokenManagerOnAllSurfaces (§9.3): one TokenManager logs in once and
// its tokens authenticate REST, gRPC (authorization metadata) and the WS
// upgrade (access_token query parameter). Also: bad credentials fail, and
// against an auth-disabled server Token is a no-op.
func TestTokenManagerOnAllSurfaces(t *testing.T) {
	env := newTestEnv(t, true, 0)
	ctx := context.Background()

	tm := NewTokenManager(env.httpAddr, Credentials{Username: auth.BootstrapAdmin, Password: "boot-pw"})

	// REST via the token manager.
	if err := NewRESTClient(env.httpAddr, WithRESTTokenFunc(tm.Token)).Ping(ctx); err != nil {
		t.Fatalf("REST Ping with token: %s", err)
	}

	// gRPC with the bearer metadata from the same manager.
	g, err := DialGRPC(env.grpcAddr, WithGRPCTokenFunc(tm.Token))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	if v, err := g.Exec("PING"); err != nil || v.Str != "PONG" {
		t.Fatalf("gRPC PING with token = %v, %v", v, err)
	}
	// Without a token the interceptor rejects (Unauthenticated).
	g2, err := DialGRPC(env.grpcAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g2.Close() }()
	if _, err := g2.ExecGeneric(ctx, "PING"); err == nil || !strings.Contains(err.Error(), "Unauthenticated") {
		t.Fatalf("unauthenticated gRPC = %v, want Unauthenticated", err)
	}

	// WS upgrade with the token as access_token.
	rec := &pushRecorder{}
	w, err := DialWS(WSOptions{Addr: env.httpAddr, TokenProvider: tm.Token, OnPush: rec.handler()})
	if err != nil {
		t.Fatalf("WS dial with token: %s", err)
	}
	defer func() { _ = w.Close() }()
	if v, err := w.Set("m7:a:k", "v"); err != nil || v.Str != "OK" {
		t.Fatalf("WS SET with token = %v, %v", v, err)
	}
	// Without a token the upgrade is rejected with 401.
	if _, err := DialWS(WSOptions{Addr: env.httpAddr}); err == nil {
		t.Fatal("WS dial without token succeeded")
	}

	// Bad credentials fail the login.
	bad := NewTokenManager(env.httpAddr, Credentials{Username: auth.BootstrapAdmin, Password: "nope"})
	if _, err := bad.Token(ctx); err == nil {
		t.Fatal("login with wrong password succeeded")
	}
}

// TestTokenManagerAuthDisabled: on a server with auth.enabled=false the
// probe makes Token a no-op ("") and the surfaces work tokenless.
func TestTokenManagerAuthDisabled(t *testing.T) {
	env := newTestEnv(t, false, 0)
	tm := NewTokenManager(env.httpAddr, Credentials{})
	tok, err := tm.Token(context.Background())
	if err != nil || tok != "" {
		t.Fatalf("Token on auth-disabled server = %q, %v; want \"\"", tok, err)
	}
}

// TestTokenRefreshRotation: forcing a refresh (expired-access simulation)
// rotates the pair and keeps the surfaces working; the token manager
// refreshes proactively based on the JWT exp claim.
func TestTokenRefreshRotation(t *testing.T) {
	env := newTestEnv(t, true, 0)
	ctx := context.Background()
	tm := NewTokenManager(env.httpAddr, Credentials{Username: auth.BootstrapAdmin, Password: "boot-pw"})
	tok1, err := tm.Token(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Force the proactive-refresh path: backdate the cached expiry.
	tm.mu.Lock()
	tm.exp = time.Now().Add(-time.Minute)
	tm.mu.Unlock()
	tok2, err := tm.Token(ctx)
	if err != nil {
		t.Fatalf("refresh: %s", err)
	}
	if tok2 == tok1 {
		t.Fatal("refresh did not rotate the access token")
	}
	// The rotated token works.
	rc := NewRESTClient(env.httpAddr, WithRESTToken(tok2))
	if err := rc.Ping(ctx); err != nil {
		t.Fatalf("Ping with rotated token: %s", err)
	}
}

// TestUmbrellaClient wires all surfaces through one Client (§11.1).
func TestUmbrellaClient(t *testing.T) {
	env := newTestEnv(t, true, 0)
	c := New(Options{
		RespAddr: env.respAddr,
		GrpcAddr: env.grpcAddr,
		HTTPAddr: env.httpAddr,
		Username: auth.BootstrapAdmin,
		Password: "boot-pw",
		DB:       -1,
	})
	defer func() { _ = c.Close() }()

	rc, err := c.RESP()
	if err != nil {
		t.Fatal(err)
	}
	if v, err := rc.Exec("SET", "m7:u:k", "1"); err != nil || v.Str != "OK" {
		t.Fatalf("RESP SET = %v, %v", v, err)
	}

	g, err := c.GRPC()
	if err != nil {
		t.Fatal(err)
	}
	if s, _ := AsString(asVal(t)(g.Get("m7:u:k"))); s != "1" {
		t.Fatalf("gRPC Get = %q", s)
	}

	w, err := c.WS()
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := AsInt(asVal(t)(w.Incr("m7:u:k"))); n != 2 {
		t.Fatalf("WS Incr = %d", n)
	}

	rest := c.REST()
	preview, err := rest.GetKey(context.Background(), "m7:u:k", 0)
	if err != nil || preview.Value != "2" {
		t.Fatalf("REST GetKey = %+v, %v", preview, err)
	}
}

// TestRenderValue pins the redis-cli-style rendering (§6.4).
func TestRenderValue(t *testing.T) {
	cases := []struct {
		v    resp.Value
		want string
	}{
		{resp.Simple("OK"), "OK"},
		{resp.Err("ERR unknown command"), "(error) ERR unknown command"},
		{resp.Int(42), "(integer) 42"},
		{resp.BlobStr("hi"), `"hi"`},
		{resp.BlobString(nil), "(nil)"},
		{resp.Null(), "(nil)"},
		{resp.Double(3.14), "(double) 3.14"},
		{resp.Bool(true), "(true)"},
		{resp.Arr(resp.BlobStr("a"), resp.Int(2)), "1) \"a\"\n2) (integer) 2"},
		{resp.Arr(resp.Arr(resp.BlobStr("x"))), "1) 1) \"x\""},
		{resp.Arr(), "(empty array)"},
		{resp.Map(resp.BlobStr("f"), resp.BlobStr("v")), "1# \"f\" => \"v\""},
		{resp.Set(resp.BlobStr("s")), "1~ \"s\""},
		{resp.Push(resp.BlobStr("message"), resp.BlobStr("ch"), resp.BlobStr("p")),
			"1) \"message\"\n2) \"ch\"\n3) \"p\""},
		{resp.Value{Kind: resp.KindBigNumber, Str: "3492890328409238509324850943850943825024385"},
			"(big number) 3492890328409238509324850943850943825024385"},
	}
	for i, tc := range cases {
		if got := Format(tc.v); got != tc.want {
			t.Errorf("case %d: Format = %q, want %q", i, got, tc.want)
		}
	}
}

// TestSplitShell pins the REPL line grammar.
func TestSplitShell(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{`SET k v`, []string{"SET", "k", "v"}},
		{`SET k "hello world"`, []string{"SET", "k", "hello world"}},
		{`SET k 'it''s'`, []string{"SET", "k", "its"}},
		{`SET k "a\nb"`, []string{"SET", "k", "a\nb"}},
		{`SET k a\ b`, []string{"SET", "k", "a b"}},
		{`  `, nil},
		{`SET k ""`, []string{"SET", "k", ""}},
	}
	for _, tc := range cases {
		got, err := SplitShell(tc.in)
		if err != nil {
			t.Errorf("SplitShell(%q): %s", tc.in, err)
			continue
		}
		if len(got) != len(tc.want) {
			t.Errorf("SplitShell(%q) = %v, want %v", tc.in, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("SplitShell(%q) = %v, want %v", tc.in, got, tc.want)
				break
			}
		}
	}
	if _, err := SplitShell(`SET k "unterminated`); err == nil {
		t.Error("unterminated double quote: no error")
	}
	if _, err := SplitShell(`SET k 'unterminated`); err == nil {
		t.Error("unterminated single quote: no error")
	}
}
