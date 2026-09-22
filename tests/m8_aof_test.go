package tests

// M8 scripting persistence test (decision S2, §13.1): a script's effects
// replicate to the AOF per-command (EVAL itself is never logged), so a
// crash + restart restores exactly the keyspace the scripts produced.
// Uses the m5 subprocess pattern (SIGKILL, same-dir restart).

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestM8AofEffectsRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("crash-recovery test needs the ultima-server binary (-short)")
	}
	dir := t.TempDir()
	addr, stop := m5StartServer(t, dir, true) // appendonly, appendfsync always
	c := m3Dial(t, addr)

	if got := c.do(t, "EVAL", "redis.call('set','s1','v1') redis.call('incr','n1') redis.call('incr','n1') return redis.call('lpush','l1','a','b')", "0"); got != ":2" {
		t.Fatalf("script 1: %s", got)
	}
	if got := c.do(t, "EVAL", "return redis.call('set','t1','tv','PX','600000')", "0"); got != "+OK" {
		t.Fatalf("script set PX: %s", got)
	}
	// A blocking pop from a script logs its plain effect (LPOP).
	if got := c.do(t, "EVAL", "redis.call('rpush','l2','x','y') return redis.call('blpop','l2',0)", "0"); got != "*(l2 x)" {
		t.Fatalf("script blpop: %s", got)
	}
	// SPOP from a script logs SREM of the replied member.
	c.do(t, "EVAL", "redis.call('sadd','sp','m1','m2') return redis.call('spop','sp')", "0")
	// EVAL inside MULTI: the script's inner commands are captured.
	c.do(t, "MULTI")
	c.do(t, "EVAL", "return redis.call('set','txk','txv')", "0")
	c.do(t, "EXEC")
	// Another logical DB via SELECT on the connection.
	c.do(t, "SELECT", "3")
	c.do(t, "EVAL", "return redis.call('set','db3k','db3v')", "0")
	c.do(t, "SELECT", "0")

	// Live state before the crash.
	type kq struct{ args []string }
	checks := []kq{
		{[]string{"GET", "s1"}}, {[]string{"GET", "n1"}}, {[]string{"LRANGE", "l1", "0", "-1"}},
		{[]string{"GET", "t1"}}, {[]string{"LRANGE", "l2", "0", "-1"}},
		{[]string{"SMEMBERS", "sp"}}, {[]string{"GET", "txk"}},
	}
	live := make([]string, len(checks))
	for i, q := range checks {
		live[i] = c.do(t, q.args...)
	}
	if got := c.do(t, "PTTL", "t1"); got == "nil" || strings.HasPrefix(got, "-") {
		t.Fatalf("t1 lost its TTL: %s", got)
	}
	c3 := m3Dial(t, addr)
	c3.do(t, "SELECT", "3")
	liveDb3 := c3.do(t, "GET", "db3k")
	_ = c3.conn.Close()

	stop(true) // SIGKILL — no shutdown snapshot, AOF must carry everything

	// The AOF must contain no EVAL verb (S2: effects only).
	aofDir := filepath.Join(dir, "data", "appendonlydir")
	entries, err := os.ReadDir(aofDir)
	if err != nil || len(entries) == 0 {
		t.Fatalf("AOF dir %s: %v (entries %d)", aofDir, err, len(entries))
	}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(aofDir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte("EVAL")) || bytes.Contains(data, []byte("eval")) {
			t.Errorf("AOF %s contains an EVAL verb (S2: effects-only)", e.Name())
		}
	}

	addr2, stop2 := m5StartServer(t, dir, true)
	defer stop2(false)
	c2 := m3Dial(t, addr2)
	for i, q := range checks {
		if got := c2.do(t, q.args...); got != live[i] {
			t.Errorf("after restart %v = %q, want %q", q.args, got, live[i])
		}
	}
	if got := c2.do(t, "PTTL", "t1"); got == "nil" || strings.HasPrefix(got, "-") {
		t.Errorf("t1 TTL after restart: %s", got)
	}
	c4 := m3Dial(t, addr2)
	c4.do(t, "SELECT", "3")
	if got := c4.do(t, "GET", "db3k"); got != liveDb3 {
		t.Errorf("db3 key after restart = %q, want %q", got, liveDb3)
	}
	_ = c4.conn.Close()
}
