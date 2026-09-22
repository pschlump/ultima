package differential

import (
	"os"
	"testing"
)

// TestDifferentialP0 runs the full P0 script table against Ultima and a
// real redis-server (REDIS_BIN, default "redis-server") and diffs every
// reply, including error strings.
func TestDifferentialP0(t *testing.T) {
	if testing.Short() || os.Getenv("DIFFERENTIAL") == "0" {
		t.Skip("differential harness disabled (-short or DIFFERENTIAL=0)")
	}
	runScripts(t, p0Scripts, "")
}

// TestDifferentialP1 runs the M2 collection script table (hashes, lists,
// sets, sorted sets) against Ultima and a real redis-server and diffs
// every reply, including error strings.
func TestDifferentialP1(t *testing.T) {
	if testing.Short() || os.Getenv("DIFFERENTIAL") == "0" {
		t.Skip("differential harness disabled (-short or DIFFERENTIAL=0)")
	}
	runScripts(t, p1Scripts, "")
}

// TestDifferentialM3 runs the M3 script table (transactions; pub/sub and
// blocking land in later parts) against Ultima and a real redis-server and
// diffs every reply, including error strings.
func TestDifferentialM3(t *testing.T) {
	if testing.Short() || os.Getenv("DIFFERENTIAL") == "0" {
		t.Skip("differential harness disabled (-short or DIFFERENTIAL=0)")
	}
	runScripts(t, m3Scripts, "")
}

// TestDifferentialM5 runs the M5a keyspace-notification script table
// (CONFIG parity, __keyevent/__keyspace streams, class gating, edge
// no-event cases, expired, blocking wakes) against Ultima and a real
// redis-server and diffs every reply and push frame.
func TestDifferentialM5(t *testing.T) {
	if testing.Short() || os.Getenv("DIFFERENTIAL") == "0" {
		t.Skip("differential harness disabled (-short or DIFFERENTIAL=0)")
	}
	runScripts(t, m5Scripts, "")
}

// TestDifferentialAuth runs the requirepass script table against a
// password-protected pair of servers.
func TestDifferentialAuth(t *testing.T) {
	if testing.Short() || os.Getenv("DIFFERENTIAL") == "0" {
		t.Skip("differential harness disabled (-short or DIFFERENTIAL=0)")
	}
	runScripts(t, authScripts, "diff-secret-pw")
}

// TestDifferentialM8 runs the M8 scripting script table (EVAL/EVALSHA/
// EVAL_RO/SCRIPT, conversions, redis.call, BUSY/KILL) against Ultima and
// a real redis-server and diffs every reply, including error strings.
func TestDifferentialM8(t *testing.T) {
	if testing.Short() || os.Getenv("DIFFERENTIAL") == "0" {
		t.Skip("differential harness disabled (-short or DIFFERENTIAL=0)")
	}
	runScripts(t, m8Scripts, "")
}

var authScripts = []script{
	{"requirepass-flow", []step{
		cmd("RECONNECT"), // fresh unauthenticated connections on both sides
		cmd("PING"),
		cmd("GET", "x"),
		cmd("SET", "x", "1"),
		cmd("AUTH", "wrong"),
		cmd("AUTH", "default", "wrong"),
		cmd("AUTH", "nouser", "diff-secret-pw"),
		cmd("AUTH", "diff-secret-pw"),
		cmd("PING"),
		cmd("SET", "x", "1"),
		cmd("GET", "x"),
		cmd("RECONNECT"),
		cmd("AUTH", "default", "diff-secret-pw"),
		cmd("GET", "x"),
		cmd("RECONNECT"),
		cmdM(mHello, "HELLO", "3", "AUTH", "default", "diff-secret-pw"),
		cmd("GET", "x"),
		cmd("RECONNECT"),
		cmd("HELLO", "3", "AUTH", "default", "wrong"),
	}},
}
