package ultima

import (
	"context"
	"testing"

	"github.com/pschlump/ultima/lib/auth"
)

// TestRESTManagement covers the unauthenticated management endpoints
// (§10.1): info, shards, scan/key browsing, config get/put.
func TestRESTManagement(t *testing.T) {
	env := newTestEnv(t, false, 0)
	ctx := context.Background()
	rc := NewRESTClient(env.httpAddr)

	if err := rc.Ping(ctx); err != nil {
		t.Fatalf("Ping: %s", err)
	}

	// Seed data through the RESP surface.
	c, err := DialRESP(env.respAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, err := c.Exec("MSET", "m7:rest:a", "1", "m7:rest:b", "2"); err != nil {
		t.Fatal(err)
	}

	info, err := rc.Info(ctx)
	if err != nil {
		t.Fatalf("Info: %s", err)
	}
	if _, ok := info.Sections["server"]["redis_version"]; !ok {
		t.Fatalf("Info missing server.redis_version: %v", info.Sections["server"])
	}

	shards, err := rc.Shards(ctx)
	if err != nil || len(shards) == 0 {
		t.Fatalf("Shards = %v, %v", shards, err)
	}
	total := 0
	for _, s := range shards {
		total += s.Keys
	}
	if total < 2 {
		t.Fatalf("shard key total = %d, want >= 2", total)
	}

	scan, err := rc.Scan(ctx, "0", "m7:rest:*", 10, 0)
	if err != nil {
		t.Fatalf("Scan: %s", err)
	}
	if len(scan.Keys) != 2 {
		t.Fatalf("Scan keys = %v, want 2", scan.Keys)
	}

	preview, err := rc.GetKey(ctx, "m7:rest:a", 0)
	if err != nil {
		t.Fatalf("GetKey: %s", err)
	}
	if preview.Type != "string" || preview.Value != "1" {
		t.Fatalf("GetKey = %+v", preview)
	}
	if _, err := rc.GetKey(ctx, "m7:rest:missing", 0); err == nil {
		t.Fatal("GetKey on missing key succeeded, want 404")
	} else if re, ok := err.(*RESTError); !ok || re.StatusCode != 404 {
		t.Fatalf("GetKey missing err = %v, want 404 RESTError", err)
	}

	deleted, err := rc.DeleteKey(ctx, "m7:rest:a", 0)
	if err != nil || !deleted {
		t.Fatalf("DeleteKey = %v, %v", deleted, err)
	}

	cfg, err := rc.GetConfig(ctx)
	if err != nil {
		t.Fatalf("GetConfig: %s", err)
	}
	if _, ok := cfg["maxmemory"]; !ok {
		t.Fatalf("GetConfig missing maxmemory: %v", cfg)
	}
	if err := rc.PutConfig(ctx, map[string]string{"maxmemory": "0"}); err != nil {
		t.Fatalf("PutConfig: %s", err)
	}
}

// TestRESTAuthFlow covers the auth endpoints with the TokenManager doing
// login: the bootstrap admin logs in, lists users, and creates a data
// account (§9.3/§9.5).
func TestRESTAuthFlow(t *testing.T) {
	env := newTestEnv(t, true, 0)
	ctx := context.Background()

	// No token: gated.
	if err := NewRESTClient(env.httpAddr).Ping(ctx); err == nil {
		t.Fatal("ungated Ping succeeded")
	}

	tm := NewTokenManager(env.httpAddr, Credentials{Username: auth.BootstrapAdmin, Password: "boot-pw"})
	rc := NewRESTClient(env.httpAddr, WithRESTTokenFunc(tm.Token))
	if err := rc.Ping(ctx); err != nil {
		t.Fatalf("authed Ping: %s", err)
	}

	users, err := rc.ListUsers(ctx)
	if err != nil || len(users) != 1 {
		t.Fatalf("ListUsers = %v, %v", users, err)
	}
	view, err := rc.CreateUser(ctx, "m7dave", "pw1", "data")
	if err != nil || view.Username != "m7dave" || view.Class != "data" {
		t.Fatalf("CreateUser = %+v, %v", view, err)
	}
	users, err = rc.ListUsers(ctx)
	if err != nil || len(users) != 2 {
		t.Fatalf("ListUsers after create = %v, %v", users, err)
	}

	// The data user logs in but cannot touch admin routes.
	daveTm := NewTokenManager(env.httpAddr, Credentials{Username: "m7dave", Password: "pw1"})
	dave := NewRESTClient(env.httpAddr, WithRESTTokenFunc(daveTm.Token))
	if err := dave.Ping(ctx); err != nil {
		t.Fatalf("dave Ping: %s", err)
	}
	if _, err := dave.ListUsers(ctx); err == nil {
		t.Fatal("data user listed users")
	} else if re, ok := err.(*RESTError); !ok || re.StatusCode != 403 {
		t.Fatalf("dave ListUsers err = %v, want 403", err)
	}

	disabled := true
	if _, err := rc.UpdateUser(ctx, "m7dave", "", &disabled); err != nil {
		t.Fatalf("UpdateUser: %s", err)
	}
	if err := rc.DeleteUser(ctx, "m7dave"); err != nil {
		t.Fatalf("DeleteUser: %s", err)
	}
}
