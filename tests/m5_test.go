package tests

// M5c crash-recovery tests (design doc §13.1): boot ./ultima-server as a
// subprocess against a temp-dir config, load data, SIGKILL, restart
// against the same dir, and assert data + TTLs survive — snapshot-only,
// AOF, and AOF-after-BGREWRITEAOF variants. Requires the built binary
// (make build); skipped under -short.

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// m5FreePort grabs an ephemeral port (listen + close).
func m5FreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

// m5StartServer boots ultima-server with a minimal config rooted at dir
// and waits for the RESP port to answer PING. The returned stopper kills
// (SIGKILL) or gracefully stops (SIGTERM) the process.
func m5StartServer(t *testing.T, dir string, appendOnly bool) (addr string, stop func(kill bool)) {
	t.Helper()
	bin := filepath.Join("..", "ultima-server")
	if _, err := os.Stat(bin); err != nil {
		t.Skipf("ultima-server binary not found (run make build): %v", err)
	}
	respPort := m5FreePort(t)
	cfg := fmt.Sprintf(`{
  "server": { "resp_addr": "127.0.0.1:%d", "grpc_addr": "127.0.0.1:%d", "http_addr": "127.0.0.1:%d", "requirepass": "" },
  "persist": { "dir": %q, "appendonly": %t, "appendfsync": "always", "save": "" }
}`, respPort, m5FreePort(t), m5FreePort(t), filepath.Join(dir, "data"), appendOnly)
	cfgPath := filepath.Join(dir, "cfg.json")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "-cfg", cfgPath)
	logf, err := os.Create(filepath.Join(dir, "server.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		t.Fatalf("start ultima-server: %v", err)
	}
	addr = fmt.Sprintf("127.0.0.1:%d", respPort)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if c, err := m3DialE(addr); err == nil {
			_ = c.conn.Close()
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			t.Fatalf("ultima-server did not come up on %s (log: %s)", addr, logf.Name())
		}
		time.Sleep(25 * time.Millisecond)
	}
	return addr, func(kill bool) {
		if kill {
			_ = cmd.Process.Signal(syscall.SIGKILL)
		} else {
			_ = cmd.Process.Signal(syscall.SIGTERM)
		}
		_ = cmd.Wait()
		_ = logf.Close()
	}
}

// m5LoadFixtures writes one of each value type plus a TTL'd key and a
// db-3 key.
func m5LoadFixtures(t *testing.T, addr string) {
	t.Helper()
	c := m3Dial(t, addr)
	if got := c.do(t, "SET", "m5s", "hello"); got != "+OK" {
		t.Fatalf("SET: %s", got)
	}
	if got := c.do(t, "RPUSH", "m5l", "a", "b", "c"); got != ":3" {
		t.Fatalf("RPUSH: %s", got)
	}
	if got := c.do(t, "SADD", "m5set", "x", "y"); got != ":2" {
		t.Fatalf("SADD: %s", got)
	}
	if got := c.do(t, "ZADD", "m5z", "1.5", "m1", "2.5", "m2"); got != ":2" {
		t.Fatalf("ZADD: %s", got)
	}
	if got := c.do(t, "HSET", "m5h", "f1", "v1", "f2", "v2"); got != ":2" {
		t.Fatalf("HSET: %s", got)
	}
	if got := c.do(t, "SET", "m5t", "ttlval", "PX", "600000"); got != "+OK" {
		t.Fatalf("SET PX: %s", got)
	}
	if got := c.do(t, "SELECT", "3"); got != "+OK" {
		t.Fatalf("SELECT: %s", got)
	}
	if got := c.do(t, "SET", "m5db3", "v3"); got != "+OK" {
		t.Fatalf("SET db3: %s", got)
	}
}

// m5CheckFixtures asserts m5LoadFixtures' data survived (db 0 and db 3).
func m5CheckFixtures(t *testing.T, addr string) {
	t.Helper()
	c := m3Dial(t, addr)
	checks := []struct {
		want string
		args []string
	}{
		{"hello", []string{"GET", "m5s"}},
		{"*(a b c)", []string{"LRANGE", "m5l", "0", "-1"}},
		{":2", []string{"SCARD", "m5set"}},
		{"2.5", []string{"ZSCORE", "m5z", "m2"}},
		{"v2", []string{"HGET", "m5h", "f2"}},
		{":6", []string{"DBSIZE"}},
	}
	for _, chk := range checks {
		if got := c.do(t, chk.args...); got != chk.want {
			t.Errorf("%s = %s, want %s", strings.Join(chk.args, " "), got, chk.want)
		}
	}
	pttl := c.do(t, "PTTL", "m5t")
	n, err := strconv.Atoi(strings.TrimPrefix(pttl, ":"))
	if err != nil || n <= 0 {
		t.Errorf("PTTL m5t after recovery = %s, want a positive TTL", pttl)
	}
	if got := c.do(t, "SELECT", "3"); got != "+OK" {
		t.Fatalf("SELECT 3: %s", got)
	}
	if got := c.do(t, "GET", "m5db3"); got != "v3" {
		t.Errorf("GET m5db3 = %s, want v3", got)
	}
}

func TestM5cSnapshotCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("crash-recovery test")
	}
	dir := t.TempDir()
	addr, stop := m5StartServer(t, dir, false)
	m5LoadFixtures(t, addr)
	c := m3Dial(t, addr)
	if got := c.do(t, "SAVE"); got != "+OK" {
		t.Fatalf("SAVE: %s", got)
	}
	stop(true) // SIGKILL: no shutdown save; only the explicit SAVE counts

	addr, stop = m5StartServer(t, dir, false)
	defer stop(false)
	m5CheckFixtures(t, addr)
}

func TestM5cAOFCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("crash-recovery test")
	}
	dir := t.TempDir()
	addr, stop := m5StartServer(t, dir, true)
	m5LoadFixtures(t, addr)
	stop(true) // SIGKILL: appendfsync=always makes every write durable

	addr, stop = m5StartServer(t, dir, true)
	defer stop(false)
	m5CheckFixtures(t, addr)
}

func TestM5cAOFRewriteCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("crash-recovery test")
	}
	dir := t.TempDir()
	addr, stop := m5StartServer(t, dir, true)
	m5LoadFixtures(t, addr)
	c := m3Dial(t, addr)
	if got := c.do(t, "BGREWRITEAOF"); got != "+Background append only file rewriting started" {
		t.Fatalf("BGREWRITEAOF: %s", got)
	}
	time.Sleep(500 * time.Millisecond) // let the rewrite finish
	stop(true)

	addr, stop = m5StartServer(t, dir, true)
	defer stop(false)
	m5CheckFixtures(t, addr)
}
